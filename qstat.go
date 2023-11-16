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
	sqlStruct    qStruct
	Variables    map[string]string
	Method       qMethod
	Filters      []qClause
	GroupS       string
	HavingS      string
	OrderS       string
	RowLimit     int
	BeginOffset  int
	BatchMode    bool
	LockFor      string
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
const LockForShare = "SHARE"
const LockForUpdate = "UPDATE"
const LockForUpdateNoWait = "UPDATE NOWAIT"
const LockForUpdateSkipLocked = "UPDATE SKIP LOCKED"
const ConfigMySQLDateTimeFormat = "2006-01-02 15:04:05.000"
const ConfigAsNullDateTimeFormat = "0001-01-01 00:00:00.000"

var ConfigParseDateTimeFormat = "2006-01-02 15:04:05.000"

func (stat *QStat) String() string {
	return fmt.Sprintf("Query Statement {\n\tMethod:\t%v\n\tsqlStruct:\t%v\n\tGroup:\t%v\n\tHaving:\t%v\n\tOrder:\t%v\nRowCount:\t%v\nOffset:\t%v\n}\n", stat.Method, stat.sqlStruct, stat.GroupS, stat.HavingS, stat.OrderS, stat.RowLimit, stat.BeginOffset)
}

// PrepareNext will prepare the next sql query
func (stat *QStat) PrepareNext(it bool) *QStat {
	stat.preparedStmt = it

	return stat
}

func (stat *QStat) FreeLength(it bool) *QStat {
	stat.sqlStruct.freeLength = it

	return stat
}

// Table setter
// Also overwrite the $T0 variable
func (stat *QStat) Table(table string) *QStat {
	stat.Variables["$T0"] = table
	stat.sqlStruct.Table = table

	return stat
}

// TableOfFields changes all the `Table` of EACH FIELD when its original `Table` is equal to $T0
func (stat *QStat) TableOfFields(table string) *QStat {
	for _idx, _val := range stat.sqlStruct.Fields {
		if _val.Table == stat.sqlStruct.Table {
			stat.sqlStruct.Fields[_idx].Table = table
		}
	}

	return stat.Table(table)
}

func (stat *QStat) Variable(key, value string) *QStat {
	stat.Variables[key] = value

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

// IgnoreNullAt sets the i-th (start from 0)
func (stat *QStat) IgnoreNullAt(i int) *QStat {
	stat.sqlStruct.setFieldIgnoreNull(i)

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

func (stat *QStat) Set(template string, vals ...interface{}) *QStat {
	stat.sqlStruct.Sets = append(stat.sqlStruct.Sets, qClause{
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

func (stat *QStat) Self(n int, param string) *QStat {
	stat.sqlStruct.Fields[n].Self = param

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

// SetOnDuplicateKeyUpdateNCol is the Setter of the OnDuplicateKeyUpdate and DuplicateKeyUpdateCol
// colDefin := "<col>": "<col_define>" will be handled as RAW
func (stat *QStat) SetOnDuplicateKeyUpdateNCol(val bool, colDefine map[string]interface{}) *QStat {
	stat.sqlStruct.OnDuplicateKeyUpdate = val
	stat.sqlStruct.DuplicateKeyUpdateCol = colDefine

	return stat
}

// AppendBatchValue append the maps-type value when batch mode enabled
func (stat *QStat) AppendBatchValue(val map[string]interface{}) *QStat {
	stat.sqlStruct.AppendBatchValue(val)

	return stat
}

func (stat *QStat) QueryLockFor(lockType string) *QStat {
	stat.LockFor = lockType

	return stat
}

// SetModel will only analyse the model without query to database
func (stat *QStat) SetModel(model interface{}) *QStat {
	sqlStruct, err := analyseStruct(model)
	stat.sqlStruct = sqlStruct
	stat.Variables = map[string]string{}
	stat.Variables["$T0"] = stat.sqlStruct.Table
	panicErrHandle(err)

	if stat.dbc.config.DebugLvl > 3 {
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

	for _replace, _new := range stat.Variables {
		_sql = strings.ReplaceAll(_sql, fmt.Sprintf("`%s`", _replace), _replace)
		_sql = strings.ReplaceAll(_sql, _replace, fmt.Sprintf("`%s`", _new))
	}

	if stat.dbc.config.DebugLvl > 0 {
		fmt.Println("Model SQL: " + _sql)
	}

	if stat.dbc.config.DebugLvl > 1 {
		fmt.Printf("Values %#v\n", stat.sqlStruct.Values)
	}

	switch stat.Method {
	case sqlBatchInsert:
		fallthrough
	case sqlBatchUpdate:
		fallthrough
	case sqlInsert:
		fallthrough
	case sqlDelete:
		fallthrough
	case sqlUpdate:
		if stat.sqlStruct.QueryOnly {
			return &QResult{
				Error: errors.New("dataq: query only"),
			}
		}

		var (
			rawResult sql.Result
		)

		if len(_sql) == 0 {
			return &QResult{}
		}

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
		if stat.dbc.config.DebugLvl > 0 {
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
			rowValue  reflect.Value
		)

		if stat.sqlStruct.freeLength && stat.sqlStruct.Value.Cap() == 0 {
			stat.sqlStruct.Value.Set(reflect.MakeSlice(stat.sqlStruct.Value.Type(), 0, 20))
		}

		for rawRows.Next() {
			rawRows.Scan(tmpDS...)

			rowValue = reflect.New(stat.sqlStruct.getElemType()).Elem()

			for i := range stat.sqlStruct.Fields {
				switch rowValue.Field(i).Kind() {
				case reflect.Bool:
					boolVal, err := strconv.ParseBool(string(values[i]))
					if err != nil {
						boolVal = false
					}
					rowValue.Field(i).SetBool(boolVal)
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					i64, err := strconv.ParseInt(string(values[i]), 10, rowValue.Field(i).Type().Bits())
					if err != nil {
						i64 = 0
					}
					rowValue.Field(i).SetInt(i64)
				case reflect.Float32, reflect.Float64:
					f64, err := strconv.ParseFloat(string(values[i]), rowValue.Field(i).Type().Bits())
					if err != nil {
						f64 = 0.0
					}
					rowValue.Field(i).SetFloat(f64)
				case reflect.String:
					if values[i] == nil {
						rowValue.Field(i).SetString("")
					} else {
						rowValue.Field(i).SetString(string(values[i]))
					}
				case reflect.Struct:
					// TODO: not only parse Time
					_type := rowValue.Field(i).Type()
					switch _type.PkgPath() {
					case "time":
						// Need timezone and can be parsed by Javascript
						t, _ := time.Parse(ConfigParseDateTimeFormat, string(values[i]))
						rowValue.Field(i).Set(reflect.ValueOf(t))
					case "github.com/collatzc/dataq":
						switch _type.Name() {
						case "QBool":
							boolVal, err := strconv.ParseBool(string(values[i]))
							if err != nil {
								boolVal = false
							}
							rowValue.Field(i).Set(reflect.ValueOf(QBool{
								Valid: true,
								Value: boolVal,
							}))
						case "QInt":
							intVal, err := strconv.Atoi(string(values[i]))
							if err != nil {
								intVal = 0
							}
							rowValue.Field(i).Set(reflect.ValueOf(QInt{
								Valid: true,
								Value: intVal,
							}))
						case "QFloat64":
							f64, err := strconv.ParseFloat(string(values[i]), 64)
							if err != nil {
								f64 = 0.0
							}
							rowValue.Field(i).Set(reflect.ValueOf(QFloat64{
								Valid: true,
								Value: f64,
							}))
						case "QString":
							rowValue.Field(i).Set(reflect.ValueOf(QString{
								Valid: true,
								Value: string(values[i]),
							}))
						case "QTime":
							t, _ := time.Parse(ConfigParseDateTimeFormat, string(values[i]))
							rowValue.Field(i).Set(reflect.ValueOf(QTime{
								Valid: true,
								Value: t,
							}))
						}
					}
				case reflect.Map:
					var _map map[string]interface{}
					err := json.Unmarshal(values[i], &_map)
					if err != nil {
						return &QResult{
							Error: err,
						}
					}
					rowValue.Field(i).Set(reflect.ValueOf(_map))
				case reflect.Slice:
					if len(values[i]) > 0 {
						var _ValueSlice = reflect.New(rowValue.Field(i).Type())
						err := json.Unmarshal(values[i], _ValueSlice.Interface())
						if err != nil {
							return &QResult{
								Error: err,
							}
						}
						rowValue.Field(i).Set(_ValueSlice.Elem())
					} else {
						rowValue.Field(i).Set(reflect.MakeSlice(rowValue.Field(i).Type(), 0, 0))
					}
				}
			}
			if stat.sqlStruct.freeLength {
				stat.sqlStruct.Value.Set(reflect.Append(*stat.sqlStruct.Value, rowValue))
			} else {
				stat.sqlStruct.getRowValue(rowNumber).Set(rowValue)
			}

			rowNumber++
		}

		if stat.dbc.config.DebugLvl > 0 {
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
			stat.QueryRowUnsafe(_sql, stat.sqlStruct.Values...).Scan(&res.ReturnedRows)
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
	if stat.sqlStruct.Length == 0 && !stat.sqlStruct.freeLength {
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
		} else if !stat.sqlStruct.freeLength {
			sql.WriteString(" LIMIT ?")
			stat.sqlStruct.Values = append(stat.sqlStruct.Values, stat.sqlStruct.Length)
		}

		if stat.BeginOffset != 0 {
			sql.WriteString(" OFFSET ?")
			stat.sqlStruct.Values = append(stat.sqlStruct.Values, stat.BeginOffset)
		}

		if stat.LockFor != "" {
			sql.WriteString(fmt.Sprintf(" FOR %s", stat.LockFor))
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
	case sqlDelete:
		sql.WriteString(stat.sqlStruct.composeDeleteSQL(stat.Filters))
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

// Delete return *QResult
func (stat *QStat) Delete() *QResult {
	stat.Method = sqlDelete

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

func (stat *QStat) QueryRowUnsafe(query string, args ...interface{}) (row *sql.Row) {
	if stat.dbc.tx != nil {
		row = stat.dbc.tx.QueryRow(query, args...)
	} else {
		row = stat.dbc.db.QueryRow(query, args...)
	}

	return
}

func (stat *QStat) ExecUnsafe(query string, args ...interface{}) (sql.Result, error) {
	if stat.dbc.tx != nil {
		return stat.dbc.tx.Exec(query, args...)
	} else {
		return stat.dbc.db.Exec(query, args...)
	}
}
