package dataq

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

// QStat ...
// Support multiple value states
type QStat struct {
	dbc         *DConn
	Method      qMethod
	Tx          *sql.Tx
	err         error
	sqlStruct   qStruct
	Filters     []string
	GroupS      string
	HavingS     string
	OrderS      string
	RowLimit    int
	BeginOffset int
	ValCondType string
	BatchMode   bool
}

// qMethod is the basic method type
type qMethod uint

const sqlInsert qMethod = 0
const sqlSelect qMethod = 1
const sqlUpdate qMethod = 2
const sqlDelete qMethod = 3
const sqlCount qMethod = 4
const sqlBatchInsert qMethod = 5
const sqlBatchUpdate qMethod = 6

func (stat QStat) String() string {
	return fmt.Sprintf("Query Statement {\n\tsqlStruct:\t%v\n\tGroup:\t%v\n\tHaving:\t%v\n\tOrder:\t%v\nRowCount:\t%v\nOffset:\t%v\n}\n", stat.sqlStruct, stat.GroupS, stat.HavingS, stat.OrderS, stat.RowLimit, stat.BeginOffset)
}

// From which table?
func (stat *QStat) From(table string) *QStat {
	stat.sqlStruct.Table = table

	return stat
}

// Join ...
func (stat *QStat) Join(joins ...string) *QStat {
	stat.sqlStruct.Joins = append(stat.sqlStruct.Joins, joins...)

	return stat
}

// Where ...
func (stat *QStat) Where(wheres ...string) *QStat {
	stat.Filters = append(stat.Filters, wheres...)

	return stat
}

// ClearWhere ...
func (stat *QStat) ClearWhere() *QStat {
	stat.Filters = nil

	return stat
}

// Disjunktion ...
func (stat *QStat) Disjunktion() *QStat {
	stat.ValCondType = " OR "
	return stat
}

// Konjunktion ...
func (stat *QStat) Konjunktion() *QStat {
	stat.ValCondType = " AND "

	return stat
}

// GroupBy ...
func (stat *QStat) GroupBy(keys string) *QStat {
	stat.GroupS = fmt.Sprintf("GROUP BY %v", keys)

	return stat
}

// Having ...
func (stat *QStat) Having(having string) *QStat {
	stat.HavingS = having

	return stat
}

// OrderBy ...
func (stat *QStat) OrderBy(order string) *QStat {
	stat.OrderS = order

	return stat
}

// Limit the query LIMIT row_count
func (stat *QStat) Limit(limit int) *QStat {
	stat.RowLimit = limit

	return stat
}

// Offset the query OFFSET offset
func (stat *QStat) Offset(offset int) *QStat {
	stat.BeginOffset = offset
	return stat
}

// Begin a transaction
func (stat *QStat) Begin() *QStat {
	stat.Tx, stat.err = stat.dbc.db.Begin()

	return stat
}

// Commit a transaction
func (stat *QStat) Commit() error {
	return stat.Tx.Commit()
}

// Rollback a transaction
func (stat *QStat) Rollback() error {
	return stat.Tx.Rollback()
}

// SetBatchMode is the Setter of the BatchMode
func (stat *QStat) SetBatchMode(val bool) *QStat {
	stat.BatchMode = val
	return stat
}

// SetOnDuplicateKeyUpdate is the Setter of the OnDuplicateKeyUpdate
func (stat *QStat) SetOnDuplicateKeyUpdate(val bool) *QStat {
	stat.sqlStruct.OnDuplicateKeyUpdate = val
	return stat
}

// SetOnDuplicateKeyUpdateNCol is the Setter of the OnDuplicateKeyUpdate and DuplicateKeyUpdateCol
func (stat *QStat) SetOnDuplicateKeyUpdateNCol(val bool, colDefine map[string]interface{}) *QStat {
	stat.sqlStruct.OnDuplicateKeyUpdate = val
	stat.sqlStruct.DuplicateKeyUpdateCol = colDefine
	return stat
}

// SetDuplicateKeyUpdateCol is the Setter of the DuplicateKeyUpdateCol
func (stat *QStat) SetDuplicateKeyUpdateCol(colDefine map[string]interface{}) *QStat {
	stat.sqlStruct.DuplicateKeyUpdateCol = colDefine
	return stat
}

// AppendBatchValue append the maps-type value when batch mode enabled
func (stat *QStat) AppendBatchValue(val map[string]interface{}) *QStat {
	stat.sqlStruct.AppendBatchValue(val)
	return stat
}

// SetModel will only analyse the model without query to database
func (stat *QStat) SetModel(model interface{}) *QStat {
	var err error
	stat.sqlStruct, err = analyseStruct(model)
	panicErrHandle(err)

	if stat.dbc.debugLvl > 1 {
		fmt.Println("=== Model Struct ===")
		fmt.Println(stat.sqlStruct)
	}

	return stat
}

// Model changes the model
func (stat *QStat) Model(model interface{}) *QStat {
	stat.SetModel(model)

	return stat
}

// Exec the query
func (stat *QStat) Exec() *QResult {

	_sql := stat.composeSQL()

	if stat.dbc.debugLvl > 0 {
		fmt.Println("Model SQL: " + _sql)
	}

	switch stat.Method {
	case sqlBatchInsert:
		fallthrough
	case sqlBatchUpdate:
		fallthrough
	case sqlInsert:
		fallthrough
	case sqlUpdate:
		if stat.sqlStruct.QueryOnly == true {
			return &QResult{
				Error: errors.New("dataq: Query only"),
			}
		}

		var (
			rawResult sql.Result
			err       error
		)
		if stat.Tx != nil {
			rawResult, err = stat.Tx.Exec(_sql)
		} else {
			rawResult, err = stat.dbc.db.Exec(_sql)
		}
		if err != nil {
			return &QResult{
				Error: err,
			}
		}
		affectedRows, err := rawResult.RowsAffected()
		if err != nil {
			return &QResult{
				Error: err,
			}
		}
		if affectedRows < int64(stat.sqlStruct.Length) {
			affectedRows = int64(stat.sqlStruct.Length)
		}

		lastInsertID, err := rawResult.LastInsertId()
		if err != nil {
			return &QResult{
				Error: err,
			}
		}
		if stat.dbc.debugLvl > 0 {
			fmt.Println("QResult: AffectedRows [", affectedRows, "] LastInsertID [", lastInsertID, "]")
		}

		return &QResult{
			AffectedRows: affectedRows,
			LastInsertId: lastInsertID,
		}
	case sqlSelect:
		if stat.sqlStruct.Value.Kind() != reflect.Slice && !stat.sqlStruct.Value.CanSet() {
			return &QResult{
				Error: errors.New("dataq: This struct not settable, use new() to init an empty struct"),
			}
		}

		var (
			nField  = len(stat.sqlStruct.Fields)
			tmpDS   []interface{}
			rawRows *sql.Rows
			err     error
		)

		if stat.sqlStruct.Value.Kind() == reflect.Slice {
			stat.Limit(stat.sqlStruct.Value.Len())
		} else {
			stat.Limit(1)
		}

		tmpDS = make([]interface{}, nField)
		values := make([]sql.RawBytes, nField)
		for i := 0; i < nField; i++ {
			tmpDS[i] = &values[i]
		}
		if stat.Tx != nil {
			rawRows, err = stat.Tx.Query(_sql)
		} else {
			rawRows, err = stat.dbc.db.Query(_sql)
		}
		if err != nil {
			return &QResult{
				Error: err,
			}
		}
		defer rawRows.Close()

		var (
			rowNumber = 0
			modField  reflect.Value
		)

		for rawRows.Next() {
			rawRows.Scan(tmpDS...)
			// assign the values to model
			for i, _field := range stat.sqlStruct.Fields {

				modField = stat.sqlStruct.getValue(_field.ValIdx, rowNumber)

				switch modField.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					i64, err := strconv.ParseInt(string(values[i]), 10, modField.Type().Bits())
					if err != nil {
						i64 = 0
					}
					modField.SetInt(i64)
				case reflect.Float32, reflect.Float64:
					f64, err := strconv.ParseFloat(string(values[i]), modField.Type().Bits())
					if err != nil {
						f64 = 0.0
					}
					modField.SetFloat(f64)
				case reflect.String:
					if values[i] == nil {
						modField.SetString("")
					} else {
						modField.SetString(string(values[i]))
					}
				case reflect.Struct:
					// TODO: not only parse Time
					if _type := modField.Type(); _type.PkgPath() == "time" && _type.Name() == "Time" {
						t, _ := time.Parse(DateTimeFormat, string(values[i]))
						modField.Set(reflect.ValueOf(t))
					}
				}
			}
			rowNumber++
		}
		if stat.dbc.debugLvl > 0 {
			fmt.Println("QResult: ReturnedRows [", rowNumber, "]")
		}
		return &QResult{
			ReturnedRows: int64(rowNumber),
		}
	case sqlCount:
		res := QResult{}
		stat.dbc.db.QueryRow(_sql).Scan(&res.ReturnedRows)

		return &res
	}

	return &QResult{}
}

func (stat *QStat) composeSQL() string {
	if stat.sqlStruct.Length == 0 {
		panic(errors.New("dataq: qStat has no table name"))
	}
	var (
		sql string
	)

	switch stat.Method {
	case sqlInsert:
		sql = stat.sqlStruct.composeInsertSQL()
	case sqlBatchInsert:
		sql = stat.sqlStruct.composeBatchInsertSQL()
	case sqlSelect:
		sql = stat.sqlStruct.composeSelectSQL(stat.ValCondType, stat.Filters)

		if stat.GroupS != "" {
			sql += fmt.Sprintf(" %v", stat.GroupS)
		}

		if stat.HavingS != "" {
			sql += fmt.Sprintf(" HAVING %v", stat.HavingS)
		}

		if stat.OrderS != "" {
			sql += fmt.Sprintf(" ORDER BY %v", stat.OrderS)
		}

		if stat.sqlStruct.Length == 1 {
			sql += " LIMIT 1"
		} else if stat.RowLimit != 0 {
			sql += fmt.Sprintf(" LIMIT %v", stat.RowLimit)
		}

		if stat.BeginOffset != 0 {
			sql += fmt.Sprintf(" OFFSET %v", stat.BeginOffset)
		}
	case sqlCount:
		sql = stat.sqlStruct.composeCountSQL(stat.ValCondType, stat.Filters)
		hasGroupBy := false
		if stat.GroupS != "" {
			sql += fmt.Sprintf(" %v", stat.GroupS)
			hasGroupBy = true
		}

		if stat.HavingS != "" {
			sql += fmt.Sprintf(" HAVING %v", stat.HavingS)
		}

		if stat.OrderS != "" {
			sql += fmt.Sprintf(" ORDER BY %v", stat.OrderS)
		}

		if stat.BeginOffset != 0 {
			sql += fmt.Sprintf(" OFFSET %v", stat.BeginOffset)
		}
		if hasGroupBy {
			sql = fmt.Sprintf("SELECT COUNT(1) FROM (%s) AS c", sql)
		}
	case sqlUpdate:
		sql = stat.sqlStruct.composeUpdateSQL(stat.ValCondType, stat.Filters, stat.RowLimit)
	case sqlBatchUpdate:
		sql = stat.sqlStruct.composeBatchUpdateSQL()
	}

	return sql
}

// Insert return *QResult
func (stat *QStat) Insert() *QResult {
	stat.Method = sqlInsert

	return stat.Exec()
}

// Query return *QResult
func (stat *QStat) Query() *QResult {
	stat.Method = sqlSelect

	return stat.Exec()
}

// Count the number of rows in result set
func (stat *QStat) Count() *QResult {
	stat.Method = sqlCount

	return stat.Exec()
}

// Update return *QResult
func (stat *QStat) Update() *QResult {
	stat.Method = sqlUpdate

	return stat.Exec()
}

// BatchInsert executes the multiply value insert
// `AffectedRows` returns the number of rows inserted
// `LastInsertId` returns the PK of first inserted row
func (stat *QStat) BatchInsert() *QResult {
	stat.Method = sqlBatchInsert

	return stat.Exec()
}

// BatchUpdate executes the CASE-WHEN-THEN update
// Fieldname case sensitive
func (stat *QStat) BatchUpdate() *QResult {
	stat.Method = sqlBatchUpdate

	return stat.Exec()
}
