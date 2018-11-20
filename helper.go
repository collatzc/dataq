package dataq

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

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

func getColNameTable(fieldName string, tag reflect.StructTag, prevTable string) (theCol, theTable, nextTable string) {
	theCol = tag.Get("COL")
	nextTable = tag.Get("TABLE")
	if theCol != "" {
		idxPoint := strings.Index(theCol, ".")
		if idxPoint != -1 {
			col := strings.Split(theCol, ".")
			theTable = col[0]
			theCol = col[1]
		}
	} else {
		theCol = fieldName
	}
	if theTable == "" {
		theTable = nextTable
	}
	if theTable == "" && prevTable != "" {
		theTable = prevTable
		nextTable = prevTable
	}
	if nextTable == "" {
		nextTable = prevTable
	}
	return theCol, theTable, nextTable
}

func parseInterface(typeName reflect.Type, str string) (val interface{}) {
	var err error
	switch typeName.Name() {
	case "int", "int8", "int16", "int32", "int64":
		val, err = strconv.ParseInt(str, 10, typeName.Bits())
		if err != nil {
			val = 0
		}
	case "uint", "uint8", "uint16", "uint32", "uint64":
		val, err = strconv.ParseUint(str, 10, typeName.Bits())
		if err != nil {
			val = 0
		}
	case "float32", "float64":
		val, err = strconv.ParseFloat(str, typeName.Bits())
		if err != nil {
			val = 0.0
		}
	case "bool":
		val, err = strconv.ParseBool(str)
		if err != nil {
			val = false
		}
	case "string":
		val = str
	}

	return val
}

func getAsNull(field reflect.StructField) (asNull interface{}) {
	asNulls := field.Tag.Get("ASNULL")
	if asNulls == "" {
		switch field.Type.Name() {
		case "int", "int8", "int16", "int32", "int64":
			fallthrough
		case "uint", "uint8", "uint16", "uint32", "uint64":
			asNull = 0
		case "float32", "float64":
			asNull = 0.0
		case "string":
			asNull = ""
		case "bool":
			asNull = false
		case "Time":
			asNull = "0001-01-01T00:00:00Z"
		}
	} else {
		asNull = parseInterface(field.Type, asNulls)
	}

	return asNull
}

func getAlt(field reflect.StructField) (alt interface{}) {
	if hasTag(field.Tag, "ALT") {
		alt = field.Tag.Get("ALT")
		alt = parseInterface(field.Type, alt.(string))
		return alt
	}

	return nil
}

func hasTag(tag reflect.StructTag, label string) bool {
	_, hasTag := tag.Lookup(label)
	return hasTag
}

func emptyTag(tag reflect.StructTag, label string) bool {
	return tag.Get(label) == ""
}

func isEqual(val1, val2 interface{}) bool {
	if fmt.Sprintf("%#v", val1) == fmt.Sprintf("%#v", val2) {
		return true
	}
	return false
}

// <table> {
//	<column>: <values> `<tag>`
// }
// `COL`: "TABLE.FIELD"
func analyseStruct(data interface{}) (retStruct qStruct, err error) {
	tableValues := structToValue(data)
	tableMeta := tableValues.Type()
	var (
		table    = tableMeta.Name()
		theCol   string
		theTable string
		noFrom   = false
	)
	if tableValues.Kind() != reflect.Slice {
		for i := 0; i < tableValues.NumField(); i++ {

			if hasTag(tableMeta.Field(i).Tag, "OMIT") {
				continue
			}

			theCol, theTable, table = getColNameTable(tableMeta.Field(i).Name, tableMeta.Field(i).Tag, table)

			_field := qField{
				Table:   theTable,
				ColName: theCol,
				ValIdx:  i,
				IsIndex: false,
			}

			if hasTag(tableMeta.Field(i).Tag, "NOFROM") {
				noFrom = true
			} else if i == 0 {
				retStruct.Table = table
			}

			if hasTag(tableMeta.Field(i).Tag, "RAW") {
				retStruct.QueryOnly = true
				_field.ColName = tableMeta.Field(i).Tag.Get("COL")
				_field.Table = ""
			}

			if !emptyTag(tableMeta.Field(i).Tag, "JOIN") {
				retStruct.Joins = append(retStruct.Joins, tableMeta.Field(i).Tag.Get("JOIN"))
			}
			if !emptyTag(tableMeta.Field(i).Tag, "WHERE") {
				retStruct.Wheres = append(retStruct.Wheres, tableMeta.Field(i).Tag.Get("WHERE"))
			}

			_field.AsNull = getAsNull(tableMeta.Field(i))
			_field.Alt = getAlt(tableMeta.Field(i))

			if hasTag(tableMeta.Field(i).Tag, "INDEX") {
				_field.IsIndex = true
				retStruct.Index = append(retStruct.Index, _field)
			}

			retStruct.Fields = append(retStruct.Fields, _field)
		}

		retStruct.Length = 1
		retStruct.Value = tableValues
	} else {
		if tableValues.Cap() == 0 || tableValues.Len() == 0 {
			return retStruct, errors.New("dataq: Data set is empty")
		}
		tableMeta = tableValues.Index(0).Type()
		_lenField := tableMeta.NumField()

		for i := 0; i < _lenField; i++ {

			if hasTag(tableMeta.Field(i).Tag, "OMIT") {
				continue
			}

			theCol, theTable, table = getColNameTable(tableMeta.Field(i).Name, tableMeta.Field(i).Tag, table)
			_field := qField{
				Table:   theTable,
				ColName: theCol,
				ValIdx:  i,
				IsIndex: false,
			}

			if hasTag(tableMeta.Field(i).Tag, "NOFROM") {
				noFrom = true
			} else if i == 0 {
				retStruct.Table = table
			}

			if hasTag(tableMeta.Field(i).Tag, "RAW") {
				retStruct.QueryOnly = true
				_field.ColName = tableMeta.Field(i).Tag.Get("COL")
				_field.Table = ""
			}

			if !emptyTag(tableMeta.Field(i).Tag, "JOIN") {
				retStruct.Joins = append(retStruct.Joins, tableMeta.Field(i).Tag.Get("JOIN"))
			}
			if !emptyTag(tableMeta.Field(i).Tag, "WHERE") {
				retStruct.Wheres = append(retStruct.Wheres, tableMeta.Field(i).Tag.Get("WHERE"))
			}

			_field.AsNull = getAsNull(tableMeta.Field(i))
			_field.Alt = getAlt(tableMeta.Field(i))

			retStruct.Length = tableValues.Len()

			if hasTag(tableMeta.Field(i).Tag, "INDEX") {
				_field.IsIndex = true
				retStruct.Index = append(retStruct.Index, _field)
			}

			retStruct.Fields = append(retStruct.Fields, _field)
		}
		retStruct.Value = tableValues
	}

	if noFrom == true {
		retStruct.QueryOnly = true
		retStruct.Table = ""
	}

	return retStruct, nil
}

func panicErrHandle(err error) {
	if err != nil {
		panic(err.Error())
	}
}
