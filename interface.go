package dataq

import "database/sql"

// QInterface Interface
type QInterface interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Stats() sql.DBStats
	Begin() (*sql.Tx, error)
}
