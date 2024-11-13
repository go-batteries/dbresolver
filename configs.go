package dbresolver

import "database/sql"

func SetDBDefaults(db *sql.DB) {
	db.SetMaxIdleConns(DEFAULT_MAX_IDLE_CONNECTIONS)
	db.SetMaxOpenConns(DEFAULT_MAX_OPEN_CONNECTIONS)
	db.SetConnMaxIdleTime(DEFAULT_CONN_MAX_LIFETIME)
}

func SetConfigDefaults(db *sql.DB, config *DBConfig) {
	if config.MaxIdleConnections != nil {
		db.SetMaxIdleConns(*config.MaxIdleConnections)
	}

	if config.MaxOpenConnections != nil {
		db.SetMaxOpenConns(*config.MaxOpenConnections)
	}

	if config.ConnectionMaxLifetime != nil {
		db.SetConnMaxIdleTime(*config.ConnectionMaxLifetime)
	}
}
