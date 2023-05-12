package dataq

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

type qStruct struct {
	Table                 string
	TableAlias            string
	CountOn               string
	Length                int
	Index                 []qField
	Fields                []qField
	Joins                 []string
	Wheres                []string
	Sets                  []qClause
	Value                 *reflect.Value
	Values                []interface{}
	QueryOnly             bool
	BatchValue            []map[string]interface{}
	Schema                []string
	OnDuplicateKeyUpdate  bool
	DuplicateKeyUpdateCol map[string]interface{}
	freeLength            bool
}

type qClause struct {
	Operator string
	Template string
	Values   []interface{}
}

type columnMapValue map[string]*columnValue

type columnMapPkValues map[string]map[interface{}]*columnValue

type columnValue struct {
	Stmt []string
	Pk   interface{}
	Val  []interface{}
	Type string
}

var (
	columnValuePool = sync.Pool{
		New: func() interface{} {
			return new(columnValue)
		},
	}
)

func (_s *qStruct) AppendBatchValue(val map[string]interface{}) {
	_s.BatchValue = append(_s.BatchValue, val)
}

func (_s *qStruct) ClearBatchValue() {
	_s.BatchValue = make([]map[string]interface{}, 0)
}

func (_s *qStruct) IsBatchValueEmpty() bool {
	return len(_s.BatchValue) == 0
}

func (_s *qStruct) hasIndex() bool {
	return len(_s.Index) != 0
}

func (_s *qStruct) getElemType() (ret reflect.Type) {
	if _s.Value.Kind() == reflect.Slice {
		ret = _s.Value.Type().Elem()
	} else {
		ret = _s.Value.Type()
	}

	return ret
}

func (_s *qStruct) getRowValue(idxSlice int) (ret reflect.Value) {
	if _s.Value.Kind() == reflect.Slice {
		ret = _s.Value.Index(idxSlice)
	} else {
		ret = *_s.Value
	}

	return
}

func (_s *qStruct) getValueInterface(idxField, idxArray int) (ret interface{}) {
	var typeName reflect.Type
	var thisValue reflect.Value
	if _s.Value.Kind() != reflect.Slice {
		typeName = _s.Value.Field(idxField).Type()
		thisValue = _s.Value.Field(idxField)
		ret = _s.Value.Field(idxField).Interface()
	} else {
		typeName = _s.Value.Index(idxArray).Field(idxField).Type()
		thisValue = _s.Value.Index(idxArray).Field(idxField)
		ret = _s.Value.Index(idxArray).Field(idxField).Interface()
	}

	// TODO: uint output 0x00
	switch typeName.Name() {
	case "Time":
		return ret.(time.Time).Format(ConfigMySQLDateTimeFormat)
	default:
		switch typeName.Kind() {
		case reflect.Map:
			if thisValue.Len() <= 1 {
				return "{}"
			}
			j, _ := json.Marshal(ret)
			return j
		case reflect.Slice:
			if thisValue.Len() <= 1 {
				return "[]"
			}
			j, _ := json.Marshal(ret)
			return j
		default:
			return ret
		}
	}
}

func (_s *qStruct) getValueEmptyValue(idxField, idxArray int) interface{} {
	var typeName reflect.Type
	if _s.Value.Kind() != reflect.Slice {
		typeName = _s.Value.Field(idxField).Type()
	} else {
		typeName = _s.Value.Index(idxArray).Field(idxField).Type()
	}

	switch typeName.Name() {
	case "int", "int8", "int16", "int32", "int64":
		fallthrough
	case "uint", "uint8", "uint16", "uint32", "uint64":
		return 0
	case "float32", "float64":
		return 0.0
	case "string":
		return ""
	case "bool":
		return false
	case "Time":
		return time.Now().UTC().Format(ConfigMySQLDateTimeFormat)
	default:
		switch typeName.Kind() {
		case reflect.Map:
			return "{}"
		case reflect.Slice:
			return "[]"
		}
	}
	return ""
}

func (_s *qStruct) getIndexSQL() (sql string) {
	if _s.hasIndex() {
		var where strings.Builder
		for _idx, _index := range _s.Index {
			if _idx != 0 {
				where.WriteString(" AND ")
			}

			where.WriteString(fmt.Sprintf("`%s`.`%s` IN ", _index.Table, _index.ColName))
			var in = make([]string, _s.Length)
			for i := 0; i < _s.Length; i++ {
				in[i] = "?" // fmt.Sprintf("%#v", _s.getValueInterface(_index.ValIdx, i))
				_s.Values = append(_s.Values, _s.getValueInterface(_index.ValIdx, i))
			}

			where.WriteString(fmt.Sprintf("(%s)", strings.Join(in, ", ")))
		}

		return where.String()
	}

	return ""
}

func (_s *qStruct) hasJoins() bool {
	return len(_s.Joins) != 0
}

func (_s *qStruct) hasWheres() bool {
	return len(_s.Wheres) != 0
}

func (_s *qStruct) setFieldIgnoreNull(i int) *qStruct {
	_s.Fields[i].IgnoreNull = true

	return _s
}

// If qStruct.Length > 1, make sure each element has the same columns to insert!
func (_s *qStruct) composeInsertSQL() string {
	var (
		sql    strings.Builder
		colVal = make(map[string]*columnValue)
		key    string
		col    []string
		val    []string
		cV     *columnValue
	)
	_s.Values = make([]interface{}, 0, _s.Length*len(_s.Fields))
	for i := 0; i < _s.Length; i++ {
		for _, _field := range _s.Fields {
			// ignore `TABLE`. prefix
			if !isEqual(_s.getValueInterface(_field.ValIdx, i), _field.AsNull) || _field.Init {
				key = fmt.Sprintf("`%s`", _field.ColName)
				if colVal[key] == nil {
					cV = _s.AllocColumnValue()
					cV.Stmt = make([]string, 0)
					cV.Val = make([]interface{}, 0)
				} else {
					cV = colVal[key]
				}

				if _field.Json != "" {
					cV.Type = "json"
					if _field.JsonCast {
						cV.Stmt = append(cV.Stmt, fmt.Sprintf("'%s', CAST(? AS JSON)", _field.Json))
					} else {
						cV.Stmt = append(cV.Stmt, fmt.Sprintf("'%s', ?", _field.Json))
					}
				} else {
					cV.Stmt = append(cV.Stmt, "?")
				}
				cV.Val = append(cV.Val, _s.getValueInterface(_field.ValIdx, i))
				colVal[key] = cV
			}
		}

		val = make([]string, 0, len(colVal))
		if i == 0 {
			col = make([]string, 0, len(colVal))
			for _key, _val := range colVal {
				col = append(col, _key)
				if _val.Type == "json" {
					val = append(val, fmt.Sprintf("JSON_OBJECT(%s)", strings.Join(_val.Stmt, ", ")))
				} else {
					val = append(val, _val.Stmt[0])
				}
				_s.Values = append(_s.Values, _val.Val...)
				colVal[_key].Free()
				colVal[_key] = nil
				delete(colVal, _key)
			}
			sql.WriteString(fmt.Sprintf("INSERT INTO `%s` (%s) VALUES", _s.Table, strings.Join(col, ", ")))
		} else {
			for _, _key := range col {
				if colVal[_key].Type == "json" {
					val = append(val, fmt.Sprintf("JSON_OBJECT(%s)", strings.Join(colVal[_key].Stmt, ", ")))
				} else {
					val = append(val, colVal[_key].Stmt[0])
				}
				_s.Values = append(_s.Values, colVal[_key].Val...)
				colVal[_key].Free()
				colVal[_key] = nil
				delete(colVal, _key)
			}
			sql.WriteByte(',')
		}

		sql.WriteString(fmt.Sprintf(" (%s)", strings.Join(val, ",")))
	}

	if _s.OnDuplicateKeyUpdate {
		val = make([]string, 0, len(_s.DuplicateKeyUpdateCol))
		for _col, _val := range _s.DuplicateKeyUpdateCol {
			val = append(val, fmt.Sprintf("%s=%s", _col, _val))
		}
		sql.WriteString(fmt.Sprintf(" ON DUPLICATE KEY UPDATE %s", strings.Join(val, ",")))
	}

	return sql.String()
}

func (_s *qStruct) composeBatchInsertSQL() string {
	if len(_s.BatchValue) == 0 {
		return ""
	}

	var (
		sql  strings.Builder
		col  string
		val  string
		vals string
		keys []string
	)

	for _idx, _values := range _s.BatchValue {
		val = ""

		if _idx == 0 {
			for _key := range _values {
				keys = append(keys, _key)
			}
			sort.Strings(keys)
			for _, _key := range keys {
				col += fmt.Sprintf(" `%s`,", _key)
			}
		}
		for _, _key := range keys {
			val += fmt.Sprintf(" %#v,", _values[_key])
		}
		val = val[1 : len(val)-1]
		vals += "(" + val + "), "
	}
	sql.WriteString(fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s", _s.Table, col[1:len(col)-1], vals[:len(vals)-2]))

	if _s.OnDuplicateKeyUpdate {
		val = ""
		for _col, _val := range _s.DuplicateKeyUpdateCol {
			val += fmt.Sprintf(" `%s` = %s,", _col, _val)
		}
		val = val[1 : len(val)-1]
		sql.WriteString(fmt.Sprintf(" ON DUPLICATE KEY UPDATE %s", val))
	}
	sql.WriteByte(';')

	return sql.String()
}

func (_s *qStruct) composeSelectFieldSQL() string {
	var (
		sql    strings.Builder
		fields = make([]string, 0, len(_s.Fields))
	)
	for _, _field := range _s.Fields {
		fields = append(fields, _field.SelectString())
	}

	sql.WriteString(strings.Join(fields, ", "))

	return sql.String()
}

func (_s *qStruct) composeSelectSQL(filters []qClause) string {
	var (
		sql strings.Builder
	)

	_s.Values = make([]interface{}, 0)

	sql.WriteString(fmt.Sprintf("SELECT %s", _s.composeSelectFieldSQL()))

	if _s.Table != "" {
		sql.WriteString(fmt.Sprintf(" FROM `%s`", _s.Table))
		if _s.TableAlias != "" {
			sql.WriteString(fmt.Sprintf(" AS `%s`", _s.TableAlias))
		}
	}

	if _s.hasJoins() {
		sql.WriteString(fmt.Sprintf(" %s", strings.Join(_s.Joins, " ")))
	}

	condition := _s.composeWhereIndexCondition(filters)
	if len(condition) > 0 {
		sql.WriteString(fmt.Sprintf(" WHERE %s", condition))
	}

	return sql.String()
}

func (s *qStruct) composeDeleteSQL(filters []qClause) string {
	var (
		sql strings.Builder
	)
	sql.WriteString(fmt.Sprintf("DELETE FROM `%s`", s.Table))

	condition := s.composeWhereIndexCondition(filters)
	if len(condition) > 0 {
		sql.WriteString(fmt.Sprintf(" WHERE %s", condition))
	}

	return sql.String()
}

func (_s *qStruct) composeCountSQL(filters []qClause) string {
	var (
		sql strings.Builder
	)

	if _s.CountOn == "" {
		sql.WriteString(fmt.Sprintf("SELECT COUNT(1) FROM `%s`", _s.Table))
	} else {
		sql.WriteString(fmt.Sprintf("SELECT COUNT(%s) FROM `%s`", _s.CountOn, _s.Table))
	}

	if _s.TableAlias != "" {
		sql.WriteString(fmt.Sprintf(" AS `%s`", _s.TableAlias))
	}

	if _s.hasJoins() {
		sql.WriteString(fmt.Sprintf(" %s", strings.Join(_s.Joins, " ")))
	}

	condition := _s.composeWhereIndexCondition(filters)
	if condition != "" {
		sql.WriteString(fmt.Sprintf(" WHERE %s", condition))
	}

	return sql.String()
}

func (s *qStruct) composeWhereIndexCondition(filters []qClause) string {
	var (
		condition = make([]string, 0, 3)
	)

	if s.hasWheres() {
		condition = append(condition, fmt.Sprintf("(%s)", strings.Join(s.Wheres, " AND ")))
	}
	if len(filters) != 0 {
		var sqlFilter strings.Builder
		sqlFilter.WriteByte('(')
		for _idx, _filter := range filters {
			if _idx != 0 {
				sqlFilter.WriteString(fmt.Sprintf(" %s ", _filter.Operator))
			}
			sqlFilter.WriteString(_filter.Template)
			s.Values = append(s.Values, _filter.Values...)
		}
		sqlFilter.WriteByte(')')
		condition = append(condition, sqlFilter.String())
	}
	if s.hasIndex() {
		condition = append(condition, fmt.Sprintf("(%s)", s.getIndexSQL()))
	}

	return strings.Join(condition, " AND ")
}

// IMPORTANT: Only using index, pk to update!
// UPDATE table SET Col1 = CASE pk
//
//				WHEN 1 THEN ...
//				WHEN 2 THEN ...
//				ELSE Col1
//				END,
//	...
//	WHERE pk IN (1, 2, 3);
func (_s *qStruct) composeUpdateSQL(filters []qClause, limit int) string {
	var (
		sql       strings.Builder
		updates   []string
		update    strings.Builder
		key       string
		condition []string
		hasLimit  = limit != 0
	)
	_s.Values = make([]interface{}, 0)
	// for i := 0; i < _s.Length; i++ {
	if _s.Length == 1 {
		var (
			colVal = make(columnMapValue)
			cV     *columnValue
		)
		for _, _field := range _s.Fields {
			if !_field.IsIndex && _field.Table == _s.Table {
				key = fmt.Sprintf("`%s`", _field.ColName)
				if colVal[key] == nil {
					cV = _s.AllocColumnValue()
					cV.Stmt = make([]string, 0)
				} else {
					cV = colVal[key]
				}

				if _field.Self != "" {
					cV.Stmt = append(cV.Stmt, fmt.Sprintf("%s%s", key, _field.Self))
				} else if !isEqual(_s.getValueInterface(_field.ValIdx, 0), _field.AsNull) || _field.IgnoreNull || (_field.AsClear != nil && _field.AsClear == _s.getValueInterface(_field.ValIdx, 0)) {
					if _field.Json != "" {
						cV.Type = "json"
						if _field.JsonCast {
							cV.Stmt = append(cV.Stmt, fmt.Sprintf("%s', CAST(? AS JSON)", _field.Json))
						} else if _field.JsonMerge != "" {
							cV.Stmt = append(cV.Stmt, fmt.Sprintf("%s', JSON_MERGE(%s, ?)", _field.Json, _field.JsonMerge))
						} else if _field.JsonMergePreserve != "" {
							cV.Stmt = append(cV.Stmt, fmt.Sprintf("%s', JSON_MERGE_PRESERVE(%s, ?)", _field.Json, _field.JsonMergePreserve))
						} else if _field.JsonMergePatch != "" {
							cV.Stmt = append(cV.Stmt, fmt.Sprintf("%s', JSON_MERGE_PATCH(%s, ?)", _field.Json, _field.JsonMergePatch))
						} else if _field.JsonArrayAppend != "" {
							cV.Stmt = append(cV.Stmt, fmt.Sprintf("%s', JSON_ARRAY_APPEND(%s, ?)", _field.Json, _field.JsonArrayAppend))
						} else {
							cV.Stmt = append(cV.Stmt, fmt.Sprintf("%s', ?", _field.Json))
						}
					} else if _field.JsonMerge != "" {
						cV.Stmt = append(cV.Stmt, fmt.Sprintf("JSON_MERGE(%s, ?)", _field.JsonMerge))
					} else if _field.JsonMergePreserve != "" {
						cV.Stmt = append(cV.Stmt, fmt.Sprintf("JSON_MERGE_PRESERVE(%s, ?)", _field.JsonMergePreserve))
					} else if _field.JsonMergePatch != "" {
						cV.Stmt = append(cV.Stmt, fmt.Sprintf("JSON_MERGE_PATCH(%s, ?)", _field.JsonMergePatch))
					} else if _field.JsonArrayAppend != "" {
						cV.Stmt = append(cV.Stmt, fmt.Sprintf("JSON_ARRAY_APPEND(%s, ?)", _field.JsonArrayAppend))
					} else {
						cV.Stmt = append(cV.Stmt, "?")
					}

					if _field.AsClear != nil && _field.AsClear == _s.getValueInterface(_field.ValIdx, 0) {
						cV.Val = append(cV.Val, _s.getValueEmptyValue(_field.ValIdx, 0))
					} else {
						cV.Val = append(cV.Val, _s.getValueInterface(_field.ValIdx, 0))
					}
				}
				colVal[key] = cV
			}
		}

		updates = make([]string, 0, len(colVal))

		for _key, _val := range colVal {
			update.Reset()
			if len(_val.Stmt) > 0 {
				update.WriteString(fmt.Sprintf("%s=", _key))
				if _val.Type == "json" {
					update.WriteString(fmt.Sprintf("IF(JSON_VALID(%s), JSON_SET(%s, '$.%s), JSON_OBJECT('%s))", _key, _key, strings.Join(_val.Stmt, ", '$."), strings.Join(_val.Stmt, ", '")))
					_s.Values = append(_s.Values, _val.Val...)
				} else {
					update.WriteString(_val.Stmt[0])
				}
				_s.Values = append(_s.Values, _val.Val...)
				updates = append(updates, update.String())
			}
			colVal[_key].Free()
			colVal[_key] = nil
			delete(colVal, _key)
		}

		if len(_s.Sets) != 0 {
			for _, _set := range _s.Sets {
				updates = append(updates, _set.Template)
				_s.Values = append(_s.Values, _set.Values...)
			}
		}

		if _s.hasIndex() {
			indexes := make([]string, 0)
			for _, _index := range _s.Index {
				indexes = append(indexes, fmt.Sprintf("`%s`=?", _index.ColName))
				_s.Values = append(_s.Values, _s.getValueInterface(_index.ValIdx, 0))
			}
			condition = append(condition, fmt.Sprintf("(%s)", strings.Join(indexes, " AND ")))
		}

		if _s.hasWheres() {
			condition = append(condition, fmt.Sprintf("(%s)", strings.Join(_s.Wheres, " AND ")))
		}

		if len(filters) != 0 {
			var sqlFilter strings.Builder
			sqlFilter.WriteByte('(')
			for _idx, _filter := range filters {
				if _idx != 0 {
					sqlFilter.WriteString(fmt.Sprintf(" %s ", _filter.Operator))
				}
				sqlFilter.WriteString(_filter.Template)
				_s.Values = append(_s.Values, _filter.Values...)
			}
			sqlFilter.WriteByte(')')
			condition = append(condition, sqlFilter.String())
		}

		if len(condition) > 0 {
			sql.WriteString(fmt.Sprintf("UPDATE `%s` SET %s WHERE %s", _s.Table, strings.Join(updates, ", "), strings.Join(condition, " AND ")))
			if hasLimit {
				sql.WriteString(fmt.Sprintf(" LIMIT %#v", limit))
			}
		} else if hasLimit {
			sql.WriteString(fmt.Sprintf("UPDATE `%s` SET%s LIMIT %#v", _s.Table, strings.Join(updates, ", "), limit))
		}
	} else {
		// primary key is required!
		if !_s.hasIndex() {
			panic(errors.New("dataq: multiple row update must have to specify primary key"))
		}
		// the first index as primary key
		var (
			_PK    = fmt.Sprintf("`%s`", _s.Index[0].ColName)
			ids    = make([]string, 0)
			colVal = make(columnMapPkValues)
			cV     *columnValue
		)
		for i := 0; i < _s.Length; i++ {
			var _PkVal = _s.getValueInterface(_s.Index[0].ValIdx, i)
			ids = append(ids, fmt.Sprintf("%#v", _PkVal))
			for _, _field := range _s.Fields {
				if !_field.IsIndex && _field.Table == _s.Table {
					key = fmt.Sprintf("`%s`", _field.ColName)
					if colVal[key] == nil {
						colVal[key] = make(map[interface{}]*columnValue)
					}
					if colVal[key][_PkVal] == nil {
						cV = _s.AllocColumnValue()
						cV.Stmt = make([]string, 0)
					} else {
						cV = colVal[key][_PkVal]
					}

					cV.Pk = _PkVal
					if !isEqual(_s.getValueInterface(_field.ValIdx, i), _field.AsNull) {
						if _field.Json != "" {
							cV.Type = "json"
							cV.Stmt = append(cV.Stmt, fmt.Sprintf("%s', ?", _field.Json))
						} else {
							cV.Stmt = append(cV.Stmt, "?")
						}
						cV.Val = append(cV.Val, _s.getValueInterface(_field.ValIdx, i))
					} else if _field.Self != "" {
						cV.Stmt = append(cV.Stmt, fmt.Sprintf("%s%s", key, _field.Self))
					}
					colVal[key][_PkVal] = cV
				}
			}
		}

		sql.WriteString(fmt.Sprintf("UPDATE `%s` SET ", _s.Table))
		updates = make([]string, 0, len(colVal))
		var hasStmt = false

		for _column, _pks := range colVal {
			update.Reset()
			hasStmt = false
			if len(_pks) > 0 {
				update.WriteString(fmt.Sprintf("%s=CASE %s", _column, _PK))
				for _pk, _val := range _pks {
					if len(_val.Stmt) > 0 {
						hasStmt = true
						update.WriteString(" WHEN ? THEN ")
						_s.Values = append(_s.Values, _pk)
						if _val.Type == "json" {
							update.WriteString(fmt.Sprintf("IF(JSON_VALID(%s), JSON_SET(%s, '$.%s), JSON_OBJECT('%s))", _column, _column, strings.Join(_val.Stmt, ", '%."), strings.Join(_val.Stmt, ",'")))
							_s.Values = append(_s.Values, _val.Val...)
						} else {
							update.WriteString(_val.Stmt[0])
						}
						_s.Values = append(_s.Values, _val.Val...)
					}
					_pks[_pk].Free()
					_pks[_pk] = nil
					delete(_pks, _pk)
				}
			}
			if hasStmt {
				update.WriteString(fmt.Sprintf(" ELSE %s END", _column))
				updates = append(updates, update.String())
			}
			delete(colVal, _column)
		}
		sql.WriteString(fmt.Sprintf("%s WHERE %s IN (%s)", strings.Join(updates, ","), _PK, strings.Join(ids, ",")))
	}

	return sql.String()
}

// UPDATE categories
//
//	SET display_order = CASE id
//	WHEN 1 THEN 3
//	WHEN 2 THEN 4
//	WHEN 3 THEN 5
//	END,
//	title = CASE id
//	WHEN 1 THEN 'New Title 1'
//	WHEN 2 THEN 'New Title 2'
//	WHEN 3 THEN 'New Title 3'
//	END
//	WHERE id IN (1,2,3)
func (_s *qStruct) composeBatchUpdateSQL() string {
	if len(_s.BatchValue) == 0 {
		return ""
	}

	var (
		sql           strings.Builder
		lenBatchValue = len(_s.BatchValue) - 1
		lenBatchField = len(_s.BatchValue[0]) - 1
		fieldName     = make([]string, lenBatchField)
		indexName     = _s.Index[0].ColName
		update        string
		_cond         string
		condMap       = make(map[string]bool)
		condition     string
	)

	for _idx, _values := range _s.BatchValue {
		_i := 0
		for _col, _val := range _values {
			if _col != "INDEX" {
				if _idx == 0 {
					fieldName[_i] = fmt.Sprintf("`%s` = CASE `%s` ", _col, indexName)
				}
				fieldName[_i] += fmt.Sprintf("WHEN %#v THEN %#v ", _values["INDEX"], _val)
				_cond = fmt.Sprintf("%#v", _values["INDEX"])
				if !condMap[_cond] {
					condMap[_cond] = true
				}
				if _idx == lenBatchValue {
					fieldName[_i] += "END"
				}
				_i++
			}
		}
	}

	update = strings.Join(fieldName, ", ")

	for _condition := range condMap {
		condition += _condition + ", "
	}

	sql.WriteString(fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s` IN (%s);", _s.Table, update, indexName, condition[:len(condition)-2]))

	return sql.String()
}

func (_s *qStruct) composeCreateTableSQL() string {
	var (
		sql    strings.Builder
		fields = make([]string, len(_s.Fields))
	)

	for _idx, _values := range _s.Fields {
		fields[_idx] = fmt.Sprintf("`%s` %s", _values.ColName, _values.Schema)
	}

	fieldsDef := strings.Join(fields, ", ")
	indexDef := strings.Join(_s.Schema, ", ")

	sql.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;", _s.Table, fieldsDef, indexDef))

	return sql.String()
}

func (s *qStruct) GetValues() []interface{} {
	return s.Values
}

func (_s *qStruct) String() string {
	return fmt.Sprintf("Query Struct: {\nTable:\t\t%v\nLength:\t\t%v\nIndex:\t\t%v\nFields:\t\t%v\n\tJoins:\t%v\n\tWheres:\t%v\n\tQueryOnly:\t%v\n\tBatchValue:\t%v\nSchema:\t%v\nOnDuplicateKeyUpdate: %v\nDuplicateKeyUpdateCol: %v\n}\n", _s.Table, _s.Length, _s.Index, _s.Fields, _s.Joins, _s.Wheres, _s.QueryOnly, _s.BatchValue, _s.Schema, _s.OnDuplicateKeyUpdate, _s.DuplicateKeyUpdateCol)
}

func (c qClause) String() string {
	return fmt.Sprintf("\tQuery Clause: {\n\t\tOperator:\t%v\n\t\tTemplate:\t%v\n\t\tValues:\t%v\n\t}\n", c.Operator, c.Template, c.Values)
}

func ClearValue(v interface{}) {
	p := reflect.ValueOf(v).Elem()
	p.Set(reflect.Zero(p.Type()))
}

func (_s *qStruct) AllocColumnValue() *columnValue {
	return columnValuePool.Get().(*columnValue)
}

func (x *columnValue) Free() {
	ClearValue(x)
	columnValuePool.Put(x)
}
