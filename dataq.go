package dataq

import (
	"database/sql"
	"errors"
)

// DConn Database Connection
type DConn struct {
	db       QInterface
	dbName   string
	debugLvl int
}

// Open MySQL Connection
// Open(connectionString, debugLevel)
func Open(args ...interface{}) (dbc *DConn, err error) {
	lenArgs := len(args)
	if lenArgs == 0 {
		err = errors.New("dataq: invalid database source")
	}
	var (
		connectString string
		dbConn        QInterface
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
		// TODO: need more params
		if debugLvl, ok = args[1].(int); !ok {
			debugLvl = int(args[1].(float64))
		} else {
			debugLvl = args[1].(int)
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
func (dbc *DConn) SQLDB() QInterface {
	return dbc.db
}

// Model returns a qStat object with default method SQLSelect
func (dbc *DConn) Model(model interface{}) *QStat {
	stat := QStat{
		dbc:         dbc.clone(),
		Method:      sqlSelect,
		ValCondType: " OR ",
	}
	stat.Model(model)

	return &stat
}
