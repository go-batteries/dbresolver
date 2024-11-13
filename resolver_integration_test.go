package dbresolver_test

import (
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/go-batteries/dbresolver"
	_ "github.com/mattn/go-sqlite3"
)

func Test_DatabaseWithQueryKoalescer(t *testing.T) {
	db, err := sql.Open("sqlite3", "./tmp/test.db")
	if err != nil {
		t.Fatalf("failed to open SQLite database: %v", err)
	}

	defer db.Close()

	// Create a sample table
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	db.Exec(`DELETE FROM users`)

	_, err = db.Exec(`INSERT INTO users (name) VALUES ('Alice'), ('Bob')`)
	if err != nil {
		t.Fatalf("failed to insert sample data: %v", err)
	}

	koalescer := dbresolver.NewKoalescer(&dbresolver.NoopEvictor{})
	database := dbresolver.Register(dbresolver.DBConfig{
		Master:      dbresolver.AsMaster(db, "users"),
		DefaultMode: &dbresolver.DbWriteMode,
		Replicas:    []*dbresolver.ResolverDB{dbresolver.AsSyncReplica(db, "users_read")},
	}, dbresolver.WithQueryQualescer(koalescer))

	// Simulate concurrent queries using the koalescer
	var wg sync.WaitGroup
	concurrentQueries := 5
	results := make(chan dbresolver.Rows, concurrentQueries)

	queryFunc := func() {
		defer wg.Done()
		rows, err := database.Query(`SELECT * FROM users`)
		if err != nil {
			t.Errorf("query failed: %v", err)
			return
		}

		results <- rows
	}

	// Run concurrent queries
	wg.Add(concurrentQueries)
	for i := 0; i < concurrentQueries; i++ {
		go queryFunc()
	}

	// Simulate adding a new record.
	// This should not affect the total count
	// Since result has not been evicted yet
	time.Sleep(100 * time.Millisecond)

	_, err = db.Exec(`INSERT INTO users(name) VALUES ('Paul')`)
	if err != nil {
		t.Fatal("failed to insert another value in users")
	}

	// Wait for all queries to complete
	wg.Wait()
	close(results)

	// Check that results are as expected
	var totalRows int
	for rows := range results {
		totalRows += len(rows)
	}

	expectedRows := 2
	if totalRows != concurrentQueries*expectedRows {
		t.Errorf("unexpected number of rows: got %d, want %d", totalRows, concurrentQueries*expectedRows)
	}

	_, err = db.Exec(`DELETE FROM users WHERE name = 'Paul'`)
	if err != nil {
		t.Fatal("failed to delete exisisting user from db")
	}

	rows, err := database.Query(`SELECT name FROM users`)
	if err != nil {
		t.Fatal("should not have failed to get user names")
	}

	if len(rows) != 2 {
		t.Fatal("there are more than two records for this test.")
	}

	// This should evict the old record and get data
	_, err = database.Exec(`INSERT INTO users (name) VALUES ('Jane')`)
	if err != nil {
		t.Fatal("should not have failed to insert data")
	}

	rows, err = database.Query(`SELECT * FROM users`)
	if err != nil {
		t.Fatal("should not have failed to get users")
	}

	if len(rows) != 3 {
		t.Fatal("should have a total of 3 records")
	}
}
