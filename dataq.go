package dataq

import (
	"database/sql"
	"errors"
	"sync"
	"time"
)

// QData Database Connection
type QData struct {
	db           QInterface
	dbName       string
	debugLvl     int
	tx           *sql.Tx
	preparedStmt *sync.Map
	shared       *sharedConfig
}

type sharedConfig struct {
	mux          sync.RWMutex
	preparedStmt map[string]*sql.Stmt
	store        *sync.Map
}

type dConnCloseI interface {
	Close() error
}

// Open MySQL Connection
// Open(connectionString, debugLevel)
func Open(args ...interface{}) (dbc *QData, err error) {
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

	if d, ok := dbConn.(*sql.DB); ok {
		if err := d.Ping(); err != nil {
			d.Close()
			return nil, err
		}
		d.SetMaxOpenConns(100)
		d.SetMaxIdleConns(80)
		d.SetConnMaxLifetime(time.Minute * 5)
	}

	dbConn.QueryRow("SELECT DATABASE()").Scan(&dbName)
	dbc = &QData{
		db:       dbConn,
		dbName:   dbName,
		debugLvl: debugLvl,
		shared: &sharedConfig{
			store:        &sync.Map{},
			preparedStmt: map[string]*sql.Stmt{},
		},
		preparedStmt: &sync.Map{},
	}

	return
}

// Close MySQL Connection
func (dbc *QData) Close() error {
	dbc.shared.mux.Lock()
	for query, stmt := range dbc.shared.preparedStmt {
		delete(dbc.shared.preparedStmt, query)
		stmt.Close()
	}
	dbc.shared.mux.Unlock()

	if db, ok := dbc.db.(dConnCloseI); ok {
		return db.Close()
	}
	return errors.New("dataq: Can't close current connection")
}

func (dbc *QData) clone() *QData {
	newc := QData{
		db:       dbc.db,
		dbName:   dbc.dbName,
		debugLvl: dbc.debugLvl,
		shared:   dbc.shared,
	}

	if dbc.tx != nil {
		newc.tx = dbc.tx
		newc.preparedStmt = dbc.preparedStmt
	}

	return &newc
}

// DBName return Name of Database
func (dbc *QData) DBName() string {
	return dbc.dbName
}

// DBStat return sql.DBStats
func (dbc *QData) DBStat() sql.DBStats {
	return dbc.db.Stats()
}

// SQLDB return *sql.DB
func (dbc *QData) SQLDB() QInterface {
	return dbc.db
}

// Model returns a qStat object with default method SQLSelect
func (dbc *QData) Model(model interface{}) *QStat {
	stat := QStat{
		dbc:          dbc.clone(),
		preparedStmt: false,
		Variables:    map[string]string{},
	}
	stat.Model(model)

	return &stat
}

// Begin returns a transaction handler
func (c *QData) Begin() *QData {
	newc := c.clone()
	tx, err := newc.db.Begin()
	if err != nil {
		panic(err)
	}

	newc.tx = tx
	newc.preparedStmt = &sync.Map{}

	return newc
}

// Commit will do as it named
func (c *QData) Commit() error {
	if c.tx != nil {
		err := c.tx.Commit()
		c.tx = nil

		return err
	}

	return nil
}

// Rollback will do as it named
func (c *QData) Rollback() error {
	if c.tx != nil {
		err := c.tx.Rollback()
		c.tx = nil

		return err
	}

	return nil
}
