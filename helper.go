package dataq

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// QCond ...
type QCond struct {
	Join  string
	Where string
}

// the return Value can be Kind() of Slice
func structToValue(data interface{}) *reflect.Value {
	tableValues := reflect.ValueOf(data)
	if tableValues.Kind() == reflect.Ptr {
		tableValues = reflect.Indirect(tableValues)
	}

	return &tableValues
}

// for type Slice
func structsToValue(data interface{}) *reflect.Value {
	tableValues := reflect.ValueOf(data)
	if tableValues.Kind() == reflect.Ptr {
		tableValues = reflect.Indirect(tableValues)
	}

	tableValues = tableValues.Index(0)

	return &tableValues
}

// <table> {
//	<column>: <values> `<tag>`
// }
func analyseStruct(data interface{}) (table string, column []string, alias []string, values []interface{}, primary SQLPrimary, condition QCond, model *reflect.Value, err error) {
	tableValues := structToValue(data)
	tableMeta := tableValues.Type()
	var _struct SQLStruct
	if tableValues.Kind() != reflect.Slice {
		for i := 0; i < tableValues.NumField(); i++ {
			_field := SQLField{
				ColText: tableMeta.Field(i).Tag.Get("COL"),
				Alias:   tableMeta.Field(i).Name,
				Value:   tableValues.Field(i).Interface(),
			}
			column = append(column, tableMeta.Field(i).Tag.Get("COL"))
			alias = append(alias, tableMeta.Field(i).Name)
			values = append(values, tableValues.Field(i).Interface())
			if len(tableMeta.Field(i).Tag.Get("PK")) > 0 {
				if len(tableMeta.Field(i).Tag.Get("COL")) > 0 {
					primary.Name = tableMeta.Field(i).Tag.Get("COL")
				} else {
					primary.Name = tableMeta.Field(i).Name
				}
				primary.Values = append(primary.Values, tableValues.Field(i).Interface())
			}
			// TableName
			if len(tableMeta.Field(i).Tag.Get("TABLE")) > 0 {
				table = tableMeta.Field(i).Tag.Get("TABLE")
			}
			if len(tableMeta.Field(i).Tag.Get("JOIN")) > 0 {
				condition.Join += " " + tableMeta.Field(i).Tag.Get("JOIN")
			}
			if len(tableMeta.Field(i).Tag.Get("WHERE")) > 0 {
				condition.Where += " " + tableMeta.Field(i).Tag.Get("WHERE")
			}
			_field.Table = table
			_struct.Fields = append(_struct.Fields, _field)
		}
	} else {
		if tableValues.Cap() == 0 || tableValues.Len() == 0 {
			err = errors.New("dataq: Data set is empty")
			return "", nil, nil, nil, primary, condition, nil, errors.New("dataq: Data set is empty")
		}
		tableMeta = tableValues.Index(0).Type()
		idxPK := 0
		for i := 0; i < tableMeta.NumField(); i++ {
			column = append(column, tableMeta.Field(i).Tag.Get("COL"))
			alias = append(alias, tableMeta.Field(i).Name)
			if len(tableMeta.Field(i).Tag.Get("PK")) > 0 {
				idxPK = i
				// scan PK and make a array to save the value (if has)
				if len(tableMeta.Field(i).Tag.Get("COL")) > 0 {
					primary.Name = tableMeta.Field(i).Tag.Get("COL")
				} else {
					primary.Name = tableMeta.Field(i).Name
				}
			}
			// falls tableName
			if len(tableMeta.Field(i).Tag.Get("TABLE")) > 0 {
				table = tableMeta.Field(i).Tag.Get("TABLE")
			}
			if len(tableMeta.Field(i).Tag.Get("JOIN")) > 0 {
				condition.Join = " " + tableMeta.Field(i).Tag.Get("JOIN")
			}
			if len(tableMeta.Field(i).Tag.Get("WHERE")) > 0 {
				condition.Where = " " + tableMeta.Field(i).Tag.Get("WHERE")
			}
		}

		for i := 0; i < tableValues.Len(); i++ {
			for j := 0; j < tableValues.Index(i).NumField(); j++ {
				values = append(values, tableValues.Index(i).Field(j).Interface())
			}
			primary.Values = append(primary.Values, tableValues.Index(i).Field(idxPK).Interface())
		}
	}

	if table == "" {
		table = tableMeta.Name()
	}

	return table, column, alias, values, primary, condition, tableValues, nil
}

func composeCol(col string) (sqlcolname string) {
	return fmt.Sprintf("`%s`", strings.Replace(col, ".", "`.`", -1))
}

func composeColumnsCQ(arrColumn, arrAlias []string) (columnS string) {
	for i, col := range arrColumn {
		if col != "" {
			columnS += fmt.Sprintf(", %s", composeCol(col))
		} else {
			columnS += fmt.Sprintf(", %s", composeCol(arrAlias[i]))
		}
	}
	return columnS[2:]
}

func composeValuesC(lenCol int, arrVal []interface{}) (sqlval string) {
	if len(arrVal) > lenCol {
		for i, val := range arrVal {
			if i%lenCol == 0 {
				sqlval += fmt.Sprintf("), (%#v", val)
				continue
			} else {
				sqlval += fmt.Sprintf(", %#v", val)
			}
		}
		return sqlval[3:] + ")"
	}

	for _, val := range arrVal {
		sqlval += fmt.Sprintf(", %#v", val)
	}
	return fmt.Sprintf("(%s)", sqlval[2:])
}

func composeValuesQ(arrColumn []string, arrVal []interface{}, joint string) (sqlval string) {
	var arrSQL []string
	for i, val := range arrVal {
		check := fmt.Sprintf("%#v", val)
		if check != "\"\"" && check != "0" {
			arrSQL = append(arrSQL, fmt.Sprintf("%s=%#v", composeCol(arrColumn[i]), val))
		}
	}
	return strings.Join(arrSQL, " "+joint+" ")
}

func composeColValsCSingle(arrCol []string, arrVal []interface{}) string {
	var (
		sqlCols  string
		sqlVals  string
		checkVal string
	)

	for i, val := range arrVal {
		checkVal = fmt.Sprintf("%#v", val)
		if checkVal != "\"\"" && checkVal != "map[string]interface {}(nil)" {
			sqlCols += fmt.Sprintf(", %v", composeCol(arrCol[i]))
			if strings.Contains(checkVal, "map[string]interface {}") {
				sqlVals += fmt.Sprintf(", '%s'", strings.Replace(checkVal, "map[string]interface {}", "", 1))
			} else {
				sqlVals += fmt.Sprintf(", %s", checkVal)
			}
		}
	}
	return fmt.Sprintf("(%s) VALUES (%s)", sqlCols[2:], sqlVals[2:])
}

func composeValuesU(arrCol []string, arrAli []string, arrVal []interface{}, pk SQLPrimary) (sqlset string) {
	var checkVal string
	for i, col := range arrCol {
		if arrCol[i] != pk.Name || (len(arrAli) > 0 && arrAli[i] != pk.Name) {
			if len(col) > 0 {
				sqlset += fmt.Sprintf(", %s", composeCol(col))
			} else {
				sqlset += fmt.Sprintf(", %s", composeCol(arrAli[i]))
			}
			checkVal = fmt.Sprintf("%#v", arrVal[i])
			if strings.Contains(checkVal, "map[string]interface {}") {
				sqlset += fmt.Sprintf(", '%s'", strings.Replace(checkVal, "map[string]interface {}", "", 1))
			} else {
				sqlset += fmt.Sprintf("=%s", checkVal)
			}
		}
	}
	return sqlset[2:]
}

func composeInsertSQL(obj interface{}) (table, column, values string, err error) {
	table, arrColumn, arrAlias, arrValues, _, _, _, err := analyseStruct(obj)
	lenCol := len(arrAlias)
	column = composeColumnsCQ(arrColumn, arrAlias)
	values = composeValuesC(lenCol, arrValues)

	return table, column, values, nil
}

func composeUpdateSQL(obj interface{}) (table, updates, cond string, err error) {
	table, arrColumn, arrAlias, arrValues, primary, _, _, err := analyseStruct(obj)
	for i, col := range arrColumn {
		if col != "" {
			updates += fmt.Sprintf(", `%s`", col)
		} else {
			updates += fmt.Sprintf(", `%s`", arrAlias[i])
		}
		updates += fmt.Sprintf("=%#v", arrValues[i])
	}
	updates = updates[2:]

	for i := range primary.Values {
		cond += fmt.Sprintf("`%s`=%#v", primary.Name, primary.GetVal(i))
	}

	return table, updates, cond, nil
}

func composeQuerySQL(obj interface{}) (table, column, cond, pk string, model *reflect.Value, err error) {
	table, arrColumn, arrAlias, _, primary, _, model, err := analyseStruct(obj)
	column = composeColumnsCQ(arrColumn, arrAlias)

	for i := range primary.Values {
		cond += fmt.Sprintf("`%s`=%#v", primary.Name, primary.GetVal(i))
	}

	return table, column, cond, primary.Name, model, nil
}
