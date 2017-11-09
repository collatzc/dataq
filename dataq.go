package dataq

import (
	"database/sql"
	"errors"
	"fmt"
)

// DConn Database Connection
type DConn struct {
	db       SQLI
	dbName   string
	debugLvl int
}

// Open MySQL Connection
// Open(connectionString, debugLevel)
func Open(args ...interface{}) (dbc *DConn, err error) {
	lenArgs := len(args)
	if lenArgs == 0 {
		err = errors.New("dataq: invalide database source")
	}
	var (
		connectString string
		dbConn        SQLI
		dbName        string
		debugLvl      int
		ok            bool
	)

	switch value := args[0].(type) {
	case string:
		connectString = value
		dbConn, err = sql.Open("mysql", connectString)
	}

	if lenArgs == 2 {
		// TODO need more params
		if debugLvl, ok = args[1].(int); !ok {
			debugLvl = int(args[1].(float64))
		}
	}

	// Ping to test
	if d, ok := dbConn.(*sql.DB); ok {
		if err := d.Ping(); err != nil {
			d.Close()
		}
	}
	dbConn.QueryRow("SELECT DATABASE()").Scan(&dbName)
	dbc = &DConn{
		db:       dbConn,
		dbName:   dbName,
		debugLvl: debugLvl,
	}
	return
}

type dConnCloseI interface {
	Close() error
}

// Close MySQL Connection
func (dbc *DConn) Close() error {
	if db, ok := dbc.db.(dConnCloseI); ok {
		return db.Close()
	}
	return errors.New("dataq: Can't close current connection")
}

func (dbc *DConn) clone() *DConn {
	return &DConn{
		db:       dbc.db,
		dbName:   dbc.dbName,
		debugLvl: dbc.debugLvl,
	}
}

// DBName return Name of Database
func (dbc *DConn) DBName() string {
	return dbc.dbName
}

// DBStat return sql.DBStats
func (dbc *DConn) DBStat() sql.DBStats {
	return dbc.db.Stats()
}

// SQLDB return *sql.DB
func (dbc *DConn) SQLDB() SQLI {
	return dbc.db
}

// C as Create return *QStat
func (dbc *DConn) C() *SQLStat {
	return &SQLStat{
		dbc:    dbc.clone(),
		Method: "INSERT",
	}
}

// Q as Query return *QStat
func (dbc *DConn) Q() *SQLStat {
	return &SQLStat{
		dbc:    dbc.clone(),
		Method: "SELECT",
	}
}

// U as Update return *QStat
func (dbc *DConn) U() *SQLStat {
	return &SQLStat{
		dbc:    dbc.clone(),
		Method: "UPDATE",
	}
}

// UpdatePKStruct return *QResult
// struct with PK(tag) no null
func (dbc *DConn) UpdatePKStruct(data interface{}) *QResult {
	table, updates, cond, err := composeUpdateSQL(data)
	if err != nil {
		return &QResult{
			Error: err,
		}
	}

	sql := "UPDATE `" + table + "` SET " + updates + " WHERE " + cond

	result, err := dbc.db.Exec(sql)

	if err != nil {
		return &QResult{
			Error: err,
		}
	}
	affectedRows, err := result.RowsAffected()

	return &QResult{
		AffectedRows: affectedRows,
		Error:        err,
	}
}

// SelectPKStruct () with ...
// <table> {
// 	<PK_column>: <condition> `PK:"true"`
// }
func (dbc *DConn) SelectPKStruct(data interface{}) error {
	table, column, cond, pk, model, err := composeQuerySQL(data)
	if err != nil {
		return err
	}
	var (
		array []interface{}
	)
	sql := fmt.Sprintf("SELECT %s FROM `%s` WHERE %s ORDER BY %s ASC LIMIT 1", column, table, cond, pk)

	nField := model.NumField()
	array = make([]interface{}, nField)
	for i := 0; i < nField; i++ {
		array[i] = model.Field(i).Addr().Interface()
	}
	errQueryRow := dbc.db.QueryRow(sql).Scan(array...)
	return errQueryRow
}

// InsertStruct return ...
// support batch mode
// If batch insertion, `LastInsertId` will be the first element, the last elemnt will have `LastInsertId`+`AffectedRows`-1
func (dbc *DConn) InsertStruct(data interface{}) *QResult {
	table, column, values, err := composeInsertSQL(data)
	if err != nil {
		panic(err.Error)
	}

	sql := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s", table, column, values)

	fmt.Println(sql)

	result, err := dbc.db.Exec(sql)
	if err != nil {
		return &QResult{
			Error: err,
		}
	}
	affectedRows, err := result.RowsAffected()
	if err != nil {
		return &QResult{
			Error: err,
		}
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return &QResult{
			Error: err,
		}
	}

	return &QResult{
		AffectedRows: affectedRows,
		LastInsertId: lastInsertID,
	}
}
