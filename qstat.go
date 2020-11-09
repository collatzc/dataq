package dataq

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// QStat ...
// Support multiple value states
type QStat struct {
	dbc          *QData
	preparedStmt bool
	Method       qMethod
	sqlStruct    qStruct
	Filters      []qClause
	GroupS       string
	HavingS      string
	OrderS       string
	RowLimit     int
	BeginOffset  int
	BatchMode    bool
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
const sqlCreateTable qMethod = 100

func (stat *QStat) String() string {
	return fmt.Sprintf("Query Statement {\n\tMethod:\t%v\n\tsqlStruct:\t%v\n\tGroup:\t%v\n\tHaving:\t%v\n\tOrder:\t%v\nRowCount:\t%v\nOffset:\t%v\n}\n", stat.Method, stat.sqlStruct, stat.GroupS, stat.HavingS, stat.OrderS, stat.RowLimit, stat.BeginOffset)
}

func (stat *QStat) PrepareNext(it bool) *QStat {
	stat.preparedStmt = it

	return stat
}

// Table which table?
func (stat *QStat) Table(table string) *QStat {
	stat.sqlStruct.Table = table

	return stat
}

// IndexWith sets the i-th (start from 0) Field as Index
func (stat *QStat) IndexWith(i int) *QStat {
	if !stat.sqlStruct.Fields[i].IsIndex {
		stat.sqlStruct.Fields[i].IsIndex = true
		stat.sqlStruct.Index = append(stat.sqlStruct.Index, stat.sqlStruct.Fields[i])
	}

	return stat
}

// Join ...
func (stat *QStat) Join(joins ...string) *QStat {
	stat.sqlStruct.Joins = append(stat.sqlStruct.Joins, joins...)

	return stat
}

// Where ...
func (stat *QStat) Where(operator, template string, vals ...interface{}) *QStat {
	if strings.Contains(template, ",,,,") {
		_values := make([]string, len(vals))
		for _idx := range _values {
			_values[_idx] = "?"
		}
		template = strings.Replace(template, ",,,,", strings.Join(_values, ","), 1)

	}
	stat.Filters = append(stat.Filters, qClause{
		Operator: operator,
		Template: template,
		Values:   vals,
	})

	return stat
}

// ClearWhere ...
func (stat *QStat) ClearWhere() *QStat {
	stat.Filters = make([]qClause, 0)

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

func (stat *QStat) Scope(fn func(*QStat) *QStat) *QStat {
	ret := fn(stat)
	return ret
}

func (stat *QStat) TableSchema(defs ...string) *QStat {
	stat.sqlStruct.Schema = append(stat.sqlStruct.Schema, defs...)

	return stat
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
	sqlStruct, err := analyseStruct(model)
	stat.sqlStruct = sqlStruct
	panicErrHandle(err)

	if stat.dbc.debugLvl > 3 {
		fmt.Println("=== Init Model Struct ===")
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
				Error: errors.New("dataq: query only"),
			}
		}

		var (
			rawResult sql.Result
		)

		if stat.preparedStmt {
			preparedStmt, err := stat.sqlPrepare(_sql)

			if err != nil {
				return &QResult{
					Error: err,
				}
			}

			rawResult, err = preparedStmt.Exec(stat.sqlStruct.Values...)
			if err != nil {
				return &QResult{
					Error: err,
				}
			}
		} else {
			var err error
			rawResult, err = stat.sqlExec(_sql, stat.sqlStruct.Values...)
			if err != nil {
				return &QResult{
					Error: err,
				}
			}
		}

		affectedRows, err := rawResult.RowsAffected()
		if err != nil {
			return &QResult{
				Error: err,
			}
		}
		// if affectedRows < int64(stat.sqlStruct.Length) {
		//   affectedRows = int64(stat.sqlStruct.Length)
		// }

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
				Error: errors.New("dataq: This struct is not settable, use new() to init. an empty struct"),
			}
		}

		var (
			nField  = len(stat.sqlStruct.Fields)
			tmpDS   []interface{}
			rawRows *sql.Rows
		)

		tmpDS = make([]interface{}, nField)
		values := make([]sql.RawBytes, nField)
		for i := 0; i < nField; i++ {
			tmpDS[i] = &values[i]
		}

		if stat.preparedStmt {
			preparedStmt, err := stat.sqlPrepare(_sql)
			if err != nil {
				return &QResult{
					Error: err,
				}
			}

			if stat.dbc.debugLvl > 2 {
				fmt.Printf("Values %#v\n", stat.sqlStruct.Values)
			}

			rawRows, err = preparedStmt.Query(stat.sqlStruct.Values...)
			if err != nil {
				return &QResult{
					Error: err,
				}
			}
		} else {
			var err error
			rawRows, err = stat.sqlQuery(_sql, stat.sqlStruct.Values...)
			if err != nil {
				return &QResult{
					Error: err,
				}
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
						// Need timezone and can be parsed by Javascript
						t, _ := time.Parse(time.RFC3339, string(values[i]))
						modField.Set(reflect.ValueOf(t))
					}
				case reflect.Map:
					var _map map[string]interface{}
					err := json.Unmarshal(values[i], &_map)
					if err != nil {
						return &QResult{
							Error: err,
						}
					}
					modField.Set(reflect.ValueOf(_map))
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
		if stat.preparedStmt {
			preparedStmt, err := stat.sqlPrepare(_sql)
			if err != nil {
				res.Error = err
				return &res
			}

			preparedStmt.QueryRow(stat.sqlStruct.Values...).Scan(&res.ReturnedRows)
		} else {
			stat.sqlQueryRow(_sql).Scan(&res.ReturnedRows)
		}

		return &res
	case sqlCreateTable:
		rawResult, err := stat.sqlExec(_sql)
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

		lastInsertID, err := rawResult.LastInsertId()
		if err != nil {
			return &QResult{
				Error: err,
			}
		}

		return &QResult{
			LastInsertId: lastInsertID,
			AffectedRows: affectedRows,
		}
	}

	return &QResult{}
}

func (stat *QStat) composeSQL() string {
	if stat.sqlStruct.Length == 0 {
		panic(errors.New("dataq: table name is required"))
	}
	var (
		sql strings.Builder
	)

	switch stat.Method {
	case sqlInsert:
		sql.WriteString(stat.sqlStruct.composeInsertSQL())
	case sqlBatchInsert:
		sql.WriteString(stat.sqlStruct.composeBatchInsertSQL())
	case sqlSelect:
		sql.WriteString(stat.sqlStruct.composeSelectSQL(stat.Filters))

		if stat.GroupS != "" {
			sql.WriteString(fmt.Sprintf(" %v", stat.GroupS))
		}

		if stat.HavingS != "" {
			sql.WriteString(fmt.Sprintf(" HAVING %v", stat.HavingS))
		}

		if stat.OrderS != "" {
			sql.WriteString(fmt.Sprintf(" ORDER BY %v", stat.OrderS))
		}

		if stat.sqlStruct.Length == 1 {
			sql.WriteString(" LIMIT 1")
		} else if stat.RowLimit != 0 {
			sql.WriteString(" LIMIT ?")
			stat.sqlStruct.Values = append(stat.sqlStruct.Values, stat.RowLimit)
		} else {
			sql.WriteString(" LIMIT ?")
			stat.sqlStruct.Values = append(stat.sqlStruct.Values, stat.sqlStruct.Length)
		}

		if stat.BeginOffset != 0 {
			sql.WriteString(" OFFSET ?")
			stat.sqlStruct.Values = append(stat.sqlStruct.Values, stat.BeginOffset)
		}
	case sqlCount:
		sql.WriteString(stat.sqlStruct.composeCountSQL(stat.Filters))
		hasGroupBy := false
		if stat.GroupS != "" {
			sql.WriteString(fmt.Sprintf(" %v", stat.GroupS))
			hasGroupBy = true
		}

		if stat.HavingS != "" {
			sql.WriteString(fmt.Sprintf(" HAVING %v", stat.HavingS))
		}

		if stat.OrderS != "" {
			sql.WriteString(fmt.Sprintf(" ORDER BY %v", stat.OrderS))
		}

		if stat.BeginOffset != 0 {
			sql.WriteString(" OFFSET ?")
			stat.sqlStruct.Values = append(stat.sqlStruct.Values, stat.BeginOffset)
		}
		if hasGroupBy {
			var sqltemp strings.Builder
			sqltemp.WriteString(fmt.Sprintf("SELECT COUNT(1) FROM (%s) AS c", sql.String()))
			sql = sqltemp
		}
	case sqlUpdate:
		sql.WriteString(stat.sqlStruct.composeUpdateSQL(stat.Filters, stat.RowLimit))
	case sqlBatchUpdate:
		sql.WriteString(stat.sqlStruct.composeBatchUpdateSQL())
	case sqlCreateTable:
		sql.WriteString(stat.sqlStruct.composeCreateTableSQL())
	}

	return sql.String()
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

// CreateTable creates a table defined by qStruct
// each field must have `SCHEMA` tag
func (stat *QStat) CreateTable() *QResult {
	stat.Method = sqlCreateTable

	return stat.Exec()
}

func (stat *QStat) sqlExec(_sql string, args ...interface{}) (rawResult sql.Result, err error) {
	if stat.dbc.tx != nil {
		rawResult, err = stat.dbc.tx.Exec(_sql, args...)
	} else {
		rawResult, err = stat.dbc.db.Exec(_sql, args...)
	}

	return
}

func (stat *QStat) sqlQuery(_sql string, args ...interface{}) (rawRows *sql.Rows, err error) {
	if stat.dbc.tx != nil {
		rawRows, err = stat.dbc.tx.Query(_sql, args...)
	} else {
		rawRows, err = stat.dbc.db.Query(_sql, args...)
	}

	return
}

func (stat *QStat) sqlPrepare(_sql string) (preparedStmt *sql.Stmt, err error) {
	if stat.dbc.preparedStmt != nil {
		if stmt, ok := stat.dbc.preparedStmt.Load(_sql); ok {
			preparedStmt = stmt.(*sql.Stmt)
			return
		}
	}

	stat.dbc.shared.mux.RLock()
	if stmt, ok := stat.dbc.shared.preparedStmt[_sql]; ok {
		stat.dbc.shared.mux.RUnlock()
		if stat.dbc.tx != nil {
			preparedStmt = stat.dbc.tx.Stmt(stmt)
			stat.dbc.preparedStmt.Store(_sql, preparedStmt)
		} else {
			preparedStmt = stmt
		}
		return
	}
	stat.dbc.shared.mux.RUnlock()

	stat.dbc.shared.mux.Lock()
	if stmt, ok := stat.dbc.shared.preparedStmt[_sql]; ok {
		stat.dbc.shared.mux.Unlock()
		if stat.dbc.tx != nil {
			preparedStmt = stat.dbc.tx.Stmt(stmt)
		} else {
			preparedStmt = stmt
		}
		return
	}

	preparedStmt, err = stat.dbc.db.Prepare(_sql)
	if err == nil {
		stat.dbc.shared.preparedStmt[_sql] = preparedStmt
	}
	stat.dbc.shared.mux.Unlock()

	if err == nil && stat.dbc.tx != nil {
		preparedStmt = stat.dbc.tx.Stmt(preparedStmt)
		stat.dbc.preparedStmt.Store(_sql, preparedStmt)
	}

	return
}

func (stat *QStat) sqlQueryRow(_sql string) (row *sql.Row) {
	if stat.dbc.tx != nil {
		row = stat.dbc.tx.QueryRow(_sql)
	} else {
		row = stat.dbc.db.QueryRow(_sql)
	}

	return
}
