<!--
  Title: DBResolver
  Description: Route reads to replicas and writes to master automatically, on top of database/sql. go-batteries, dbresolver
  Author: amitavaghosh1
  -->

# DBResolver

A thin layer over Go's standard `database/sql`: register one master and
N replicas, and every query is routed to the right one automatically,
write statements to master, reads to a load-balanced replica, without
threading a "which DB" decision through your call sites. Falls back
cleanly to the write DB if you have no replicas yet, so it's safe to
adopt before you actually need read scaling.

Built on `database/sql` directly (no ORM dependency), so it works with
whatever driver you're already using.

[Repo](https://github.com/go-batteries/dbresolver)


## Quick Start

#### Importing

```bash
go get github.com/go-batteries/dbresolver@latest 
```


#### Usage

```go
import (
  "github.com/go-batteries/dbresolver"
  _ "github.com/mattn/go-sqlite3"
)

func setDatabaseDefaults(db *sql.DB) {
	db.SetMaxIdleConns(MAX_IDLE_CONNECTIONS)
	db.SetConnMaxLifetime(CONN_MAX_LIFETIME)
	db.SetMaxOpenConns(MAX_OPEN_CONNECTIONS)
	db.LogMode(true)
}

func Setup() *dbresolver.Database {
    masterDB, err := sql.Open("sqlite3", "./testdbs/users_write.db")
    if err != nil {
      log.Fatal("failed to connect to db", err)
    }

    setDatabaseDefaults(masterDB)

    replicaDBs := []*dbresolver.ResolverDB{}
    
    replica, err := sql.Open("sqlite3", "./testdbs/users_read_a.db")
    if err != nil {
      log.Fatal("failed to connect to db", err)
    }

    setDatabaseDefaults(replica)
    replicaDBs = append(replicaDBs, dbresolver.AsSyncReplica(replica, "users_read_replica1"))

    replica, err = sql.Open("sqlite3", "./testdbs/users_read_b.db")
    if err != nil {
      log.Fatal("failed to connect to db", err)
    }

    setDatabaseDefaults(replica)
    replicaDBs = append(replicaDBs, dbresolver.AsReplica(replica, "users_read_replica2"))


    return dbresolver.Register(dbresolver.DBConfig{
        Master:   dbresolver.AsMaster(masterDB, "users_wrtie"),
        Replicas: replicaDBs,
        // Policy: &dbresolver.RoundRobalancer{},
        // for existing database integration, default mode to write
        // DefaultMode: &dbresolver.DbWriteMode,
        // MaxIdleConnections: dbresolver.ToPtr(30),
        // MaxOpenConnections: ,
        // ConnectionMaxLifeTime: ,
    })
}

db := Setup()

db.QueryRow(`SELECT * FROM users`)
```

### Switching data source

It is possible to provide the option to use read or write forcefully.

```go
// Use write db
db.WithMode(dbresolver.DBWriteMode).QueryRow(`SELECT * FROM users`)

// Use read db
db.WithMode(dbresolver.DBReadMode).Exec(`DELETE FROM users`) // error
db.WithMode(dbresolver.DBWriteMode).Exec(`DELETE FROM users`) // works

// if you need to work with DML queries on read instance
// use the sql.DB object

replicaDB.DB.Exec(`DELETE FROM users`);
```

It is also possible to set the default mode to read/write mode.

```go
dbresolver.DBConfig {
    DefaultMode: &dbresolver.DbWriteMode
}
```

### Load Balancing

By default we have two balancers

```go
// RoundRobalancer
dbresolver.DBConfig{
    Policy: &dbresolver.RoundRobalancer{}
}

// RandomBalancer
dbresolver.DBConfig{
    Policy: &dbresolver.RandomBalancer{}
}
```

You can provide your own load balancer. The `Balancer` interface is defined as such

```go
type Balancer interface {
    Get() int64
}
```
