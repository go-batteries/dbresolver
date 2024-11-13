package dbresolver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-batteries/dbresolver/hooks"
)

const (
	DEFAULT_MAX_IDLE_CONNECTIONS = 30
	DEFAULT_CONN_MAX_LIFETIME    = 10 * time.Minute
	DEFAULT_MAX_OPEN_CONNECTIONS = 10
)

type ResolverDB struct {
	*sql.DB

	Name     string
	IsMaster bool
	InSync   bool
	isUp     bool
}

func NewResolveDB(db *sql.DB, name string, isMaster, inSync bool) *ResolverDB {
	return &ResolverDB{
		DB: db,

		Name:     name,
		IsMaster: isMaster,
		InSync:   inSync,
	}
}

func AsMaster(db *sql.DB, name string) *ResolverDB {
	return NewResolveDB(db, name, true, true)
}

func AsReplica(db *sql.DB, name string) *ResolverDB {
	return NewResolveDB(db, name, false, false)
}

func AsSyncReplica(db *sql.DB, name string) *ResolverDB {
	return NewResolveDB(db, name, false, true)
}

func (rd *ResolverDB) UnWrap() *sql.DB {
	return rd.DB
}

func (rd *ResolverDB) CheckHealth(ctx context.Context) error {
	err := rd.DB.PingContext(ctx)
	rd.isUp = err == nil

	return err
}

type DBConfig struct {
	Master                *ResolverDB
	Replicas              []*ResolverDB
	Policy                Balancer
	DefaultMode           *DbActionMode
	MaxIdleConnections    *int
	MaxOpenConnections    *int
	ConnectionMaxLifetime *time.Duration
}

func Register(config DBConfig, opts ...DataBaseOpts) *Database {
	if config.Master == nil {
		log.Fatal("config.Master db cannot be nil")
	}

	var balancer Balancer = NewRoundRobalancer(len(config.Replicas))

	switch config.Policy.(type) {
	case *RandomBalancer:
		balancer = NewRoundRobalancer(len(config.Replicas))
	case *RoundRobalancer:
		balancer = NewRandomBalancer(len(config.Replicas))
	default:
		balancer = NewRoundRobalancer(len(config.Replicas))
	}

	config.Policy = balancer

	if config.DefaultMode == nil {
		config.DefaultMode = &DbReadMode
	}

	database := &Database{
		Config: config,
		Hooks:  hooks.NewEventStore(),
	}

	database.Config.applyConnectionConfig()

	for _, opt := range opts {
		opt(database)
	}

	return database
}

func (cfg *DBConfig) applyConnectionConfig() {
	SetConfigDefaults(cfg.Master.DB, cfg)

	for _, replica := range cfg.Replicas {
		SetConfigDefaults(replica.DB, cfg)
	}
}

// Optional CheckHealth to validate database connection health
func (cfg *DBConfig) CheckHealth(ctx context.Context) error {
	masterErr := cfg.Master.CheckHealth(ctx)
	replicaErrors := []error{}

	for _, replica := range cfg.Replicas {
		replicaErrors = append(replicaErrors, replica.CheckHealth(ctx))
	}

	if masterErr == nil && len(replicaErrors) == 0 {
		return nil
	}

	var err error
	if masterErr != nil {
		err = fmt.Errorf("master error: %v\n", masterErr)
	}

	for _, replicaErr := range replicaErrors {
		err = fmt.Errorf("replica error: %v\n", replicaErr)
	}

	return err
}

type DbActionMode string

var (
	DbWriteMode DbActionMode = "write"
	DbReadMode  DbActionMode = "read"
)

var (
	EventBeforeDBSelect string = "before::select_db"
	EventAfterDBSelect  string = "after:select_db"
	EventBeforeQueryRun string = "before::query_run"
)

var (
	ErrorInvalidDBMode = errors.New("db mode invalid for query")
	ErrorInvalidData   = errors.New("unexpected result type from query koalescer")
)

type Database struct {
	Config    DBConfig
	Hooks     hooks.EventEmitter
	koalescer *QueryKoalescer
}

type DataBaseOpts func(d *Database)

func WithQueryQualescer(koalescer *QueryKoalescer) DataBaseOpts {
	return func(d *Database) {
		d.koalescer = koalescer
	}
}

func WithHooks(eventHandler hooks.EventEmitter) DataBaseOpts {
	return func(d *Database) {
		d.Hooks = eventHandler
	}
}

func ToKey(stmt string, values ...interface{}) string {
	hashed := HashValues(values...)
	return fmt.Sprintf("%s_%s", stmt, hashed)
}

func (d *Database) WithMode(dbMode DbActionMode) *Database {
	dbConfig := d.Config
	dbConfig.DefaultMode = &dbMode

	nd := &Database{
		Config: dbConfig,
		Hooks:  d.Hooks,
	}

	return nd
}

func (d *Database) Exec(stmt string, values ...interface{}) (sql.Result, error) {
	d.Hooks.Emit(EventBeforeQueryRun, stmt, values)

	if !isDML(strings.ToLower(stmt)) {
		return d.selectSource().Exec(stmt, values...)
	}

	// If dml statement is not executed in write mode
	// throw error
	if !d.isWriteMode() {
		return nil, ErrorInvalidDBMode
	}

	defer func() {
		if d.koalescer != nil {
			d.koalescer.Forget(ToKey(stmt, values...))
		}
	}()

	return d.getMaster().Exec(stmt, values...)
}

func (d *Database) ExecContext(ctx context.Context, stmt string, values ...interface{}) (sql.Result, error) {
	d.Hooks.Emit(EventBeforeQueryRun, stmt, values)

	if !isDML(strings.ToLower(stmt)) {
		return d.selectSource().ExecContext(ctx, stmt, values...)
	}

	if !d.isWriteMode() {
		return nil, ErrorInvalidDBMode
	}

	defer func() {
		if d.koalescer != nil {
			d.koalescer.ForgetWithContext(ctx, ToKey(stmt, values...))
		}
	}()

	return d.getMaster().ExecContext(ctx, stmt, values...)
}

func (d *Database) Query(stmt string, values ...interface{}) (Rows, error) {
	d.Hooks.Emit(EventBeforeQueryRun, stmt, values)

	source := d.selectSource()
	key := ""

	if d.koalescer != nil {
		key = ToKey(stmt, values...)
	}

	if isDML(strings.ToLower(stmt)) {
		source = d.getMaster()

		if d.koalescer != nil {
			d.koalescer.Forget(key)
		}
	}

	if d.koalescer == nil {
		res, err := source.Query(stmt, values...)
		if err != nil {
			return nil, err
		}

		return ToRows(res)
	}

	resultCh := d.koalescer.DoChan(key, func() (interface{}, error) {
		res, err := source.Query(stmt, values...)
		if err != nil {
			return nil, err
		}

		return ToRows(res)
	})

	result := <-resultCh

	if result.Err != nil {
		// res, err := source.Query(stmt, values...)
		// if err != nil {
		// 	return nil, err
		// }
		//
		// return ToRows(res)
		return nil, result.Err
	}

	rows, ok := result.Val.(Rows)
	if !ok {
		return nil, fmt.Errorf("unexpected result type from query koalescer")
	}

	return rows, nil
}

func (d *Database) QueryContext(ctx context.Context, stmt string, values ...interface{}) (Rows, error) {
	d.Hooks.Emit(EventBeforeQueryRun, stmt, values)

	source := d.selectSource()
	key := ""

	if d.koalescer != nil {
		key = ToKey(stmt, values...)
	}

	if isDML(strings.ToLower(stmt)) {
		source = d.getMaster()

		if d.koalescer != nil {
			d.koalescer.ForgetWithContext(ctx, key)
		}
	}

	if d.koalescer == nil {
		res, err := source.QueryContext(ctx, stmt, values...)
		if err != nil {
			return nil, err
		}

		return ToRows(res)
	}

	resultCh := d.koalescer.DoWithContext(ctx, key, func() (interface{}, error) {
		res, err := source.QueryContext(ctx, stmt, values...)
		if err != nil {
			return nil, err
		}

		return ToRows(res)
	})

	result := <-resultCh
	if result.Err != nil {
		// res, err := source.QueryContext(ctx, stmt, values...)
		// if err != nil {
		// 	return nil, err
		// }
		// return ToRows(res)
		return nil, result.Err
	}

	rows, ok := result.Val.(Rows)
	if !ok {
		return nil, ErrorInvalidData
	}

	return rows, nil
}

func (d *Database) QueryRow(stmt string, values ...interface{}) (*Row, error) {
	d.Hooks.Emit(EventBeforeQueryRun, stmt, values)

	source := d.selectSource()
	key := ""

	if d.koalescer != nil {
		key = ToKey(stmt, values...)
	}

	if isDML(strings.ToLower(stmt)) {
		source = d.getMaster()

		if d.koalescer != nil {
			d.koalescer.Forget(key)
		}
	}

	if d.koalescer == nil {
		res, err := source.Query(stmt, values...)
		if err != nil {
			return nil, err
		}

		return ToRow(res)
	}

	resultCh := d.koalescer.DoChan(key, func() (interface{}, error) {
		res, err := source.Query(stmt, values...)
		if err != nil {
			return nil, err
		}

		return ToRow(res)
	})

	result := <-resultCh
	if result.Err != nil {
		// res, err := source.Query(stmt, values...)
		// if err != nil {
		// 	return nil, err
		// }
		//
		// return ToRow(res)
		return nil, result.Err
	}

	row, ok := result.Val.(*Row)
	if !ok {
		return nil, ErrorInvalidData
	}

	return row, nil
}

func (d *Database) QueryRowContext(ctx context.Context, stmt string, values ...interface{}) (*Row, error) {
	d.Hooks.Emit(EventBeforeQueryRun, stmt, values)

	source := d.selectSource()
	key := ""

	if d.koalescer != nil {
		key = ToKey(stmt, values...)
	}

	if isDML(strings.ToLower(stmt)) {
		source = d.getMaster()

		if d.koalescer != nil {
			d.koalescer.ForgetWithContext(ctx, key)
		}
	}

	if d.koalescer == nil {
		res, err := source.QueryContext(ctx, stmt, values...)
		if err != nil {
			return nil, err
		}

		return ToRow(res)
	}

	resultCh := d.koalescer.DoWithContext(ctx, key, func() (interface{}, error) {
		res, err := source.QueryContext(ctx, stmt, values...)
		if err != nil {
			return nil, err
		}

		return ToRow(res)
	})

	result := <-resultCh
	if result.Err != nil {
		// res, err := source.QueryContext(ctx, stmt, values...)
		// if err != nil {
		// 	return nil, err
		// }
		//
		// return ToRow(res)
		return nil, result.Err
	}

	row, ok := result.Val.(*Row)
	if !ok {
		return nil, ErrorInvalidData
	}

	return row, nil
}

func (d *Database) getReplica() (db *ResolverDB) {
	nextIdx := d.Config.Policy.Get()

	db = d.Config.Master

	if len(d.Config.Replicas) > 0 && nextIdx < int64(len(d.Config.Replicas)) {
		db = d.Config.Replicas[nextIdx]
	}

	d.Hooks.Emit(EventAfterDBSelect, "replica", db.Name, nextIdx)
	return
}

func (d *Database) getMaster() *ResolverDB {
	d.Hooks.Emit(EventAfterDBSelect, "master", d.Config.Master.Name, 0)
	return d.Config.Master
}

func (d *Database) selectSource() *ResolverDB {
	d.Hooks.Emit(EventBeforeDBSelect, *d.Config.DefaultMode)

	if DbWriteMode == *d.Config.DefaultMode {
		return d.getMaster()
	}

	return d.getReplica()
}

func (d *Database) isWriteMode() bool {
	return DbWriteMode == *d.Config.DefaultMode
}

func isDML(sql string) bool {
	sql = strings.ToLower(strings.TrimSpace(sql))

	isSelect := len(sql) > 7 && strings.EqualFold(sql[:6], "select")
	isLockQuery := strings.Contains(sql[6:], "for update") ||
		strings.Contains(sql[6:], "for share")

	if isSelect && isLockQuery {
		return true
	}

	return !isSelect
}

func ToPtr[E any](e E) *E {
	return &e
}
