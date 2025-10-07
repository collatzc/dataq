package dataq

import (
	"database/sql"
	"errors"
	"sync"
	"time"
)

const (
	DefaultConnMaxIdleTime = 5 * time.Minute
	DefaultConnMaxLifetime = 5 * time.Minute
	MaxIdleConns           = 128
	MaxOpenConns           = 128
)

// QData Database Connection
type QData struct {
	db           QInterface
	dbName       string
	tx           *sql.Tx
	preparedStmt *sync.Map
	shared       *sharedConfig
	config       Config
}

type Config struct {
	DebugLvl        int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
	MaxIdleConns    int
	MaxOpenConns    int
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
func Open(args ...any) (dbc *QData, err error) {
	lenArgs := len(args)
	if lenArgs == 0 {
		err = errors.New("dataq: invalid database source")
	}
	var (
		connectString string
		dbConn        QInterface
		dbName        string
		config        Config
		ok            bool
	)

	switch value := args[0].(type) {
	case string:
		connectString = value
		dbConn, err = sql.Open("mysql", connectString)
	}

	if lenArgs == 2 {
		// TODO: need more params
		if _, ok = args[1].(float64); ok {
			config.DebugLvl = int(args[1].(float64))
		} else if _, ok = args[1].(Config); ok {
			config = args[1].(Config)
		}
	}

	if config.ConnMaxIdleTime == 0 {
		config.ConnMaxIdleTime = DefaultConnMaxIdleTime
	}
	if config.ConnMaxLifetime == 0 {
		config.ConnMaxLifetime = DefaultConnMaxLifetime
	}
	if config.MaxOpenConns == 0 {
		config.MaxOpenConns = 128
	}
	if config.MaxIdleConns == 0 {
		config.MaxIdleConns = 128
	}

	if d, ok := dbConn.(*sql.DB); ok {
		if err := d.Ping(); err != nil {
			d.Close()
			return nil, err
		}
		d.SetMaxOpenConns(config.MaxOpenConns)
		d.SetMaxIdleConns(config.MaxIdleConns)
		d.SetConnMaxLifetime(config.ConnMaxLifetime)
		d.SetConnMaxIdleTime(config.ConnMaxIdleTime)
	}

	dbConn.QueryRow("SELECT DATABASE()").Scan(&dbName)
	dbc = &QData{
		db:     dbConn,
		dbName: dbName,
		config: config,
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
		db:           dbc.db,
		dbName:       dbc.dbName,
		shared:       dbc.shared,
		config:       dbc.config,
		preparedStmt: &sync.Map{},
	}

	if dbc.tx != nil {
		newc.tx = dbc.tx
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
func (dbc *QData) Model(model any) *QStat {
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

func (c *QData) FinAfterFuncOK(txFunc func() error) error {
	var err error
	if c.tx != nil {
		defer func() {
			if p := recover(); p != nil {
				c.tx.Rollback()
				panic(p)
			} else if err != nil {
				c.tx.Rollback()
			} else {
				err = c.tx.Commit()
			}
			c.tx = nil
		}()
	}

	if txFunc != nil {
		err = txFunc()
	}

	return err
}

func (c *QData) FinDefaultCommit() error {
	var err error

	if c.tx != nil {
		if p := recover(); p != nil {
			c.tx.Rollback()
			panic(p)
		} else {
			err = c.tx.Commit()
		}
		c.tx = nil
	}

	return err
}

func (c *QData) FinDefaultRollback() error {
	var err error

	if c.tx != nil {
		if p := recover(); p != nil {
			c.tx.Rollback()
			c.tx = nil
			panic(p)
		}
		err = c.tx.Rollback()
		c.tx = nil
	}

	return err
}

func (c *QData) QueryUnsafe(query string, args ...any) (*sql.Rows, error) {
	if c.tx != nil {
		return c.tx.Query(query, args...)
	} else {
		return c.db.Query(query, args...)
	}
}

func (c *QData) QueryRowUnsafe(query string, args ...any) (row *sql.Row) {
	if c.tx != nil {
		row = c.tx.QueryRow(query, args...)
	} else {
		row = c.db.QueryRow(query, args...)
	}

	return
}

func (c *QData) ExecUnsafe(query string, args ...any) (sql.Result, error) {
	if c.tx != nil {
		return c.tx.Exec(query, args...)
	} else {
		return c.db.Exec(query, args...)
	}
}
