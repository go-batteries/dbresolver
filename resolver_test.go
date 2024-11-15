package dbresolver

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/stretchr/testify/require"
)

func TestIsDML(t *testing.T) {
	t.Run("valid queries", func(t *testing.T) {
		createQuery := "CREATE TABLE users()"
		require.Equal(t, isDML(createQuery), true)

		selectQuery := "SELECT COUNT(1) FROM users"
		require.Equal(t, isDML(selectQuery), false)

		insertQuery := "INSERT INTO users VALUES()"
		require.Equal(t, isDML(insertQuery), true)

		updateQuery := `UPDATE users SET email="abcd@gmail.com"`
		require.Equal(t, isDML(updateQuery), true)

		deleteQuery := "DELETE FROM users"
		require.Equal(t, isDML(deleteQuery), true)

		selectShareQuery := "SELECT * FROM FOR UPDATE"
		require.Equal(t, isDML(selectShareQuery), true)

		selectShareQuery = "SELECT * FROM FOR SHARE"
		require.Equal(t, isDML(selectShareQuery), true)
	})

	t.Run("invalid queries, defaults to modification query, to allow select masteer db", func(t *testing.T) {
		invalidQuery := "FOOBAR"
		require.Equal(t, isDML(invalidQuery), true)
	})
}

func setupTestDB() (*Database, error) {
	masterDB, err := sql.Open("sqlite3", "./tmp/master.db")
	if err != nil {
		return nil, err
	}
	replicaDB, err := sql.Open("sqlite3", "./tmp/replica.db")
	if err != nil {
		return nil, err
	}

	// Create tables in master and replica to simulate behavior
	if _, err := masterDB.Exec("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT); DELETE FROM test;"); err != nil {
		return nil, err
	}

	if _, err := replicaDB.Exec("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT); DELETE FROM test;"); err != nil {
		return nil, err
	}

	return Register(
		DBConfig{
			Master:   AsMaster(masterDB, "master"),
			Replicas: []*ResolverDB{AsReplica(replicaDB, "replica")},
		},
	), nil
}

func TestExec_WriteMode(t *testing.T) {
	db, err := setupTestDB()
	if err != nil {
		t.Fatalf("failed to do whatever %v\n", err)
	}

	db = db.WithMode(DbWriteMode)

	_, err = db.Exec("INSERT INTO test (name) VALUES (?)", "write-test")
	if err != nil {
		t.Fatalf("unexpected error executing write: %v", err)
	}

	rows, err := db.Query("SELECT name FROM test")
	if err != nil {
		t.Fatalf("failed to get rows %v\n", err)
	}

	if len(rows) == 0 {
		t.Fatalf("expected row in master")
	}

	replicaRows, err := db.getReplica().DB.Query("SELECT name FROM test")
	if err != nil {
		t.Fatalf("failed to execute query from replica")
	}

	defer replicaRows.Close()
	if replicaRows.Next() {
		t.Fatalf("did not expect row in replica")
	}
}

func TestQuery_ReadMode(t *testing.T) {
	db, err := setupTestDB()
	if err != nil {
		t.Fatalf("failed to do whatever %v\n", err)
	}

	// Insert data directly into replica for testing
	db.getReplica().DB.Exec("INSERT INTO test (name) VALUES (?)", "read-test")

	rows, err := db.Query("SELECT name FROM test")
	if err != nil {
		t.Fatalf("unexpected error querying: %v", err)
	}

	if len(rows) == 0 {
		t.Fatal("expected row in replica")
	}
}

func Test_InvalidMode(t *testing.T) {
	db, err := setupTestDB()
	if err != nil {
		t.Fatalf("failed to setup db. %v\n", err)
	}

	_, err = db.Exec("INSERT INTO test (name) VALUES ('switch-test-2')")
	if err == nil {
		t.Fatal("should have failed to insert in read mode")
	}
}

func TestDBModeSwitch(t *testing.T) {
	db, err := setupTestDB()
	if err != nil {
		t.Fatalf("failed to do whatever %v\n", err)
	}

	_, err = db.WithMode(DbWriteMode).Exec("INSERT INTO test (name) VALUES (?)", "switch-test")
	if err != nil {
		t.Fatalf("unexpected error executing write: %v", err)
	}

	rows, err := db.Query("SELECT name FROM test")
	if err != nil {
		t.Fatalf("failed to select name, %v\n", err)
	}
	if len(rows) != 0 {
		t.Fatalf("did not expect row in replica")
	}

	rows, err = db.WithMode(DbWriteMode).Query("SELECT name FROM test")
	if err != nil {
		t.Fatalf("failed to select name, %v\n", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row in master")
	}

	_, err = db.WithMode(DbWriteMode).Exec("INSERT INTO test (name) VALUES (?)", "switch-test-3")
	if err != nil {
		t.Fatalf("unexpected error executing write: %v", err)
	}

	// Ensure master has inserted and result is not cached by koalescer
	rows, err = db.WithMode(DbWriteMode).Query("SELECT name FROM test")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows in master")
	}
}
