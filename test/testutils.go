package test

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func tempSqliteDB(t *testing.T) *sql.DB {
	f, _ := os.CreateTemp(".", "*.db")
	t.Cleanup(func() { os.Remove(f.Name()) })

	db, err := sql.Open("sqlite3", f.Name())
	require.NoError(t, err)

	return db
}
