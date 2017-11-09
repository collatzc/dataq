package dataq

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// SQLJoin store join statement
type SQLJoin struct {
	Inner []string
	Left  []string
	Right []string
	Join  string
}

// SQLPrimary store primary key
type SQLPrimary struct {
	Name   string
	Values []interface{}
}

// SQLStat ...
// call from DBConn
type SQLStat struct {
	dbc         *DConn
	Method      string
	TableNameS  string
	FieldA      []string
	JoinSet     SQLJoin
	WhereA      []string
	CondSQL     string
	GroupS      string
	HavingS     string
	OrderS      string
	RowCount    int
	BeginOffset int
	ValueA      []interface{}
	ValCondType string
	PrimaryM    SQLPrimary
	modValue    *reflect.Value
}

// SQLStatOpts ...
type SQLStatOpts int

// SQLOptNull ...
const SQLOptNull SQLStatOpts = 0x0

// SQLOptOnlyOne ...
const SQLOptOnlyOne SQLStatOpts = 0x01

// GetVal returns value of primary key
func (p *SQLPrimary) GetVal(idx int) interface{} {
	// TODO check exists
	return p.Values[idx]
}

func (stat *SQLStat) String() string {
	return fmt.Sprintf("Query Statement {\nTable:\t%v\nField:\t%v\nJoin:\t%v\nWhere:\t%v\nGroup:\t%v\nHaving:\t%v\nOrder:\t%v\nRowCount:\t%v\nOffset:\t%v\n}\n", stat.TableNameS, stat.FieldA, stat.JoinSet, stat.WhereA, stat.GroupS, stat.HavingS, stat.OrderS, stat.RowCount, stat.BeginOffset)
}

// From which table?
func (stat *SQLStat) From(table string) *SQLStat {
	stat.TableNameS = table
	return stat
}

// Select the query column
func (stat *SQLStat) Select(fields ...string) *SQLStat {
	stat.FieldA = append(stat.FieldA, fields...)
	return stat
}

// Join ...
func (stat *SQLStat) Join(joins ...string) *SQLStat {
	stat.JoinSet.Inner = append(stat.JoinSet.Inner, joins...)
	return stat
}

// LeftJoin ...
func (stat *SQLStat) LeftJoin(joins ...string) *SQLStat {
	stat.JoinSet.Left = append(stat.JoinSet.Left, joins...)
	return stat
}

// RightJoin ...
func (stat *SQLStat) RightJoin(joins ...string) *SQLStat {
	stat.JoinSet.Right = append(stat.JoinSet.Right, joins...)
	return stat
}

// Where ...
func (stat *SQLStat) Where(wheres ...string) *SQLStat {
	stat.WhereA = append(stat.WhereA, wheres...)
	return stat
}

// WhereSQL ...
func (stat *SQLStat) WhereSQL(sql string) *SQLStat {
	stat.CondSQL = sql
	return stat
}

// Disjunktion ...
func (stat *SQLStat) Disjunktion() *SQLStat {
	stat.ValCondType = "OR"
	return stat
}

// Konjunktion ...
func (stat *SQLStat) Konjunktion() *SQLStat {
	stat.ValCondType = "AND"
	return stat
}

// GroupBy ...
func (stat *SQLStat) GroupBy(keys string) *SQLStat {
	stat.GroupS = fmt.Sprintf("GROUP BY %v", keys)
	return stat
}

// Having ...
func (stat *SQLStat) Having(having string) *SQLStat {
	stat.HavingS = having
	return stat
}

// OrderBy ...
func (stat *SQLStat) OrderBy(order string) *SQLStat {
	stat.OrderS = order
	return stat
}

// Limit the query LIMIT row_count
func (stat *SQLStat) Limit(limit int) *SQLStat {
	stat.RowCount = limit
	return stat
}

// Offset the query OFFSET offset
func (stat *SQLStat) Offset(offset int) *SQLStat {
	stat.BeginOffset = offset
	return stat
}

// SetModel ...
// Don't need `From` `Select` any more
func (stat *SQLStat) SetModel(model interface{}) *SQLStat {
	tableName, arrColumn, arrAlias, arrValue,
		primary, cond, modelValue, err := analyseStruct(model)
	if err != nil {
		panic(err.Error())
	}

	stat.TableNameS = tableName

	stat.FieldA = nil
	for i, col := range arrColumn {
		if col != "" {
			stat.FieldA = append(stat.FieldA, col)
		} else {
			stat.FieldA = append(stat.FieldA, arrAlias[i])
		}
	}

	if len(stat.PrimaryM.Name) == 0 {
		stat.PrimaryM = primary
	}

	if cond.Join != "" {
		stat.JoinSet.Join = cond.Join
	}

	if cond.Where != "" {
		stat.WhereA = append(stat.WhereA, cond.Where)
	}

	stat.ValueA = arrValue

	stat.modValue = modelValue

	return stat
}

// ToSQL returns the JOIN-SQL
func (join SQLJoin) ToSQL() (sql string) {
	if len(join.Join) > 0 {
		sql = join.Join
	} else {
		if len(join.Inner) > 0 {
			sql += fmt.Sprintf(" INNER JOIN %s", strings.Join(join.Inner, " INNER JOIN "))
		}
		if len(join.Left) > 0 {
			sql += fmt.Sprintf(" LEFT JOIN %s", strings.Join(join.Left, " LEFT JOIN "))
		}
		if len(join.Right) > 0 {
			sql += fmt.Sprintf(" RIGHT JOIN %s", strings.Join(join.Right, " RIGHT JOIN "))
		}
	}
	return sql
}

// Models return ...
// the #model will depends on ...
func (stat *SQLStat) Models(models interface{}) *QResult {
	if len(stat.TableNameS) == 0 || len(stat.FieldA) == 0 || stat.modValue != nil {
		stat.SetModel(models)
	} else {
		stat.modelValue(models)
	}
	var (
		kind = stat.modValue.Kind()
		// length of model
		lenModel int
		// length of Field in model
		nField int
		tmpDS  []interface{}
	)

	if kind == reflect.Slice {
		lenModel = stat.modValue.Len()
		// #rows == #model
		stat.Limit(lenModel)
		nField = stat.modValue.Index(0).NumField()
	} else {
		lenModel = 1
		stat.Limit(1)
		nField = stat.modValue.NumField()
	}

	_sql := stat.composeSQL(SQLOptNull)

	if stat.dbc.debugLvl > 0 {
		fmt.Println("Models SQL: " + _sql)
	}

	switch stat.Method {
	case "INSERT":
		fallthrough
	case "UPDATE":
		rawResult, err := stat.dbc.db.Exec(_sql)
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
		if stat.dbc.debugLvl > 0 {
			fmt.Println("CU Result: Rows[", affectedRows, "] Id[", lastInsertID, "]")
		}
		return &QResult{
			AffectedRows: affectedRows,
			LastInsertId: lastInsertID,
		}
	case "SELECT":
		// rows.Scan wants '[]interface{}' as an argument, so ...
		tmpDS = make([]interface{}, nField)
		values := make([]sql.RawBytes, nField)
		for i := 0; i < nField; i++ {
			tmpDS[i] = &values[i]
		}
		rawRows, err := stat.dbc.db.Query(_sql)
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
		// Fixed: When scanned Field == NULL, then the Fields behind will also be NULL
		for rawRows.Next() {
			// // scan the values
			rawRows.Scan(tmpDS...)
			// assign the values to model
			for i, col := range values {
				if kind == reflect.Slice {
					modField = stat.modValue.Index(rowNumber).Field(i)
				} else {
					modField = stat.modValue.Field(i)
				}
				switch modField.Kind() {
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					i64, err := strconv.ParseInt(string(col), 10, modField.Type().Bits())
					if err != nil {
						i64 = 0
					}
					modField.SetInt(i64)
				case reflect.Float32, reflect.Float64:
					f64, err := strconv.ParseFloat(string(col), modField.Type().Bits())
					if err != nil {
						f64 = 0.0
					}
					modField.SetFloat(f64)
				case reflect.String:
					if col == nil {
						modField.SetString("")
					} else {
						modField.SetString(string(col))
					}
				}
			}
			rowNumber++
		}
		if stat.dbc.debugLvl > 0 {
			fmt.Println("Q Result: Rows[", rowNumber, "]")
		}
		return &QResult{
			AffectedRows: int64(rowNumber),
		}
	}

	return &QResult{}
}

func (stat *SQLStat) composeSQL(option SQLStatOpts) string {
	if stat.TableNameS == "" {
		panic(errors.New("dataq: SQLStat has no table name"))
	}
	var (
		sql      string
		lenField = len(stat.FieldA)
	)
	if lenField == 0 {
		panic(errors.New("dataq: SQLStat has no field(s) to select"))
	}

	switch stat.Method {
	case "INSERT":
		if stat.modValue.Kind() == reflect.Slice {
			sql = fmt.Sprintf("INSERT INTO `%v` (%v) VALUES %s", stat.TableNameS, composeColumnsCQ(stat.FieldA, nil), composeValuesC(len(stat.FieldA), stat.ValueA))
		} else {
			sql = fmt.Sprintf("INSERT INTO `%v` %s", stat.TableNameS, composeColValsCSingle(stat.FieldA, stat.ValueA))
		}
		break
	case "SELECT":
		sql = fmt.Sprintf("SELECT %v FROM `%v`", composeColumnsCQ(stat.FieldA, nil), stat.TableNameS)

		sql += fmt.Sprintf("%v", stat.JoinSet.ToSQL())

		if len(stat.CondSQL) > 0 {
			sql += fmt.Sprintf(" WHERE %v", stat.CondSQL)
		} else if len(stat.WhereA) > 0 {
			sql += fmt.Sprintf(" WHERE %v", strings.Join(stat.WhereA, " "+stat.ValCondType+" "))
		} else {
			whereStat := composeValuesQ(stat.FieldA, stat.ValueA, stat.ValCondType)
			if len(whereStat) > 1 {
				sql += fmt.Sprintf(" WHERE %s", composeValuesQ(stat.FieldA, stat.ValueA, stat.ValCondType))
			}
		}

		if stat.GroupS != "" {
			sql += fmt.Sprintf(" %v", stat.GroupS)
		}

		if stat.HavingS != "" {
			sql += fmt.Sprintf(" HAVING %v", stat.HavingS)
		}

		if stat.OrderS != "" {
			sql += fmt.Sprintf(" ORDER BY %v", stat.OrderS)
		}

		if option&SQLOptOnlyOne != 0 {
			if stat.RowCount == 0 {
				sql += " LIMIT 1"
			}
		}
		if stat.RowCount != 0 {
			sql += fmt.Sprintf(" LIMIT %v", stat.RowCount)
		}

		if stat.BeginOffset != 0 {
			sql += fmt.Sprintf(" OFFSET %v", stat.BeginOffset)
		}
		break
	case "UPDATE":
		lenBatch := len(stat.ValueA) / lenField

		for line := 0; line < lenBatch; line++ {
			sql += fmt.Sprintf("\nUPDATE `%s` SET %s WHERE `%s`=%#v", stat.TableNameS, composeValuesU(stat.FieldA, nil, stat.ValueA[line*lenField:(line+1)*lenField], stat.PrimaryM), stat.PrimaryM.Name, stat.PrimaryM.GetVal(line))

			if len(stat.WhereA) > 0 {
				sql += fmt.Sprintf(" %v", strings.Join(stat.WhereA, " "))
			}
			sql += ";"
		}
		break
	}

	return sql
}

func (stat *SQLStat) modelValue(model interface{}) *SQLStat {
	stat.modValue = structToValue(model)
	return stat
}
