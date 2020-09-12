package dataq

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

type qStruct struct {
	// Table name comes from the struct
	Table string
	// Length comes from the struct
	Length int
	// Index come from the struct
	Index []qField
	// Fields come from the struct
	Fields []qField
	// Joins come from both the struct or user's dynamic definition
	Joins []string
	// Wheres come from the struct
	Wheres                []string
	Value                 *reflect.Value
	QueryOnly             bool
	BatchValue            []map[string]interface{}
	Schema                []string
	OnDuplicateKeyUpdate  bool
	DuplicateKeyUpdateCol map[string]interface{}
}

func (_s qStruct) String() string {
	return fmt.Sprintf("Query Struct: {\nTable:\t\t%v\nLength:\t\t%v\nIndex:\t\t%v\nFields:\t\t%v\n\tJoins:\t%v\n\tWheres:\t%v\n\tQueryOnly:\t%v\n\tBatchValue:\t%v\nSchema:\t%v\nOnDuplicateKeyUpdate: %v\nDuplicateKeyUpdateCol: %v\n}\n", _s.Table, _s.Length, _s.Index, _s.Fields, _s.Joins, _s.Wheres, _s.QueryOnly, _s.BatchValue, _s.Schema, _s.OnDuplicateKeyUpdate, _s.DuplicateKeyUpdateCol)
}

func (_s *qStruct) AppendBatchValue(val map[string]interface{}) {
	_s.BatchValue = append(_s.BatchValue, val)
}

func (_s *qStruct) ClearBatchValue() {
	_s.BatchValue = make([]map[string]interface{}, 0)
}

func (_s qStruct) IsBatchValueEmpty() bool {
	return len(_s.BatchValue) == 0
}

func (_s qStruct) hasIndex() bool {
	return len(_s.Index) != 0
}

func (_s qStruct) getValue(idxField, idxArray int) (ret reflect.Value) {
	if _s.Value.Kind() != reflect.Slice {
		ret = _s.Value.Field(idxField)
	} else {
		ret = _s.Value.Index(idxArray).Field(idxField)
	}

	return ret
}

func (_s qStruct) getValueInterface(idxField, idxArray int) (ret interface{}) {
	var typeName reflect.Type
	if _s.Value.Kind() != reflect.Slice {
		typeName = _s.Value.Field(idxField).Type()
		ret = _s.Value.Field(idxField).Interface()
	} else {
		typeName = _s.Value.Index(idxArray).Field(idxField).Type()
		ret = _s.Value.Index(idxArray).Field(idxField).Interface()
	}
	// TODO: uint output 0x00
	switch typeName.Name() {
	case "Time":
		return ret.(time.Time).Format(DateTimeFormat)
	default:
		if typeName.Kind() == reflect.Map {
			return strings.ReplaceAll(strings.Replace(fmt.Sprintf("%#v", ret), "map[string]interface {}", "", 1), "\"", "\"")
		}
		return ret
	}
}

func (_s qStruct) getIndexSQL() (sql string) {
	if _s.hasIndex() {
		where := ""
		for _, _index := range _s.Index {
			where += fmt.Sprintf(" `%s`.`%s` IN", _index.Table, _index.ColName)
			in := ""
			for i := 0; i < _s.Length; i++ {
				in += fmt.Sprintf(" %#v,", _s.getValueInterface(_index.ValIdx, i))
			}

			where += fmt.Sprintf("(%s) AND", in[1:len(in)-1])
		}
		return where[1 : len(where)-4]
	}
	return ""
}

func (_s qStruct) hasJoins() bool {
	return len(_s.Joins) != 0
}

func (_s qStruct) hasWheres() bool {
	return len(_s.Wheres) != 0
}

func (_s qStruct) composeInsertSQL() (sql string) {
	var (
		colVal map[string][]string
		key    string
		col    []string
		val    []string
	)
	for i := 0; i < _s.Length; i++ {
		colVal = make(map[string][]string)
		for _, _field := range _s.Fields {
			// will ignore `TABLE`. prefix
			if !isEqual(_s.getValueInterface(_field.ValIdx, i), _field.AsNull) {
				key = fmt.Sprintf("`%s`", _field.ColName)
				if colVal[key] == nil {
					colVal[key] = make([]string, 0)
				}
				if _field.Json != "" {
					colVal[key] = append(colVal[key], fmt.Sprintf("'%s', %#v", _field.Json, _s.getValueInterface(_field.ValIdx, i)))
				} else {
					colVal[key] = append(colVal[key], fmt.Sprintf("%#v", _s.getValueInterface(_field.ValIdx, i)))
				}
			}
		}
		col = make([]string, 0, len(colVal))
		val = make([]string, 0, len(colVal))

		for _key, _val := range colVal {
			col = append(col, _key)
			if len(_val) > 1 {
				val = append(val, fmt.Sprintf("JSON_OBJECT(%s)", strings.Join(_val, ", ")))
			} else {
				val = append(val, strings.Join(_val, ", "))
			}
		}

		sql = fmt.Sprintf("%sINSERT INTO `%s` (%s) VALUES (%s);", sql, _s.Table, strings.Join(col, ", "), strings.Join(val, ", "))
	}

	return sql
}

func (_s qStruct) composeBatchInsertSQL() (sql string) {
	var (
		col  string
		val  string
		vals string
		keys []string
	)

	for _idx, _values := range _s.BatchValue {
		val = ""
		// TODO: sort with sort.Strings

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
	sql = fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s;", _s.Table, col[1:len(col)-1], vals[:len(vals)-2])

	if _s.OnDuplicateKeyUpdate {
		val = ""
		for _col, _val := range _s.DuplicateKeyUpdateCol {
			val += fmt.Sprintf(" `%s` = %s,", _col, _val)
		}
		val = val[1 : len(val)-1]
		sql = fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", sql[0:len(sql)-1], val)
	}

	return sql
}

func (_s qStruct) composeSelectFieldSQL() (sql string) {
	var (
		fields = make([]string, 0, len(_s.Fields))
	)
	for _, _field := range _s.Fields {
		fields = append(fields, _field.SelectString())
	}

	return strings.Join(fields, ", ")
}

func (_s qStruct) composeSelectSQL(filterType string, filters []string) (sql string) {
	condition := ""
	sql = fmt.Sprintf("SELECT %s", _s.composeSelectFieldSQL())

	if _s.Table != "" {
		sql = fmt.Sprintf("%s FROM `%s`", sql, _s.Table)
	}

	if _s.hasJoins() {
		sql = fmt.Sprintf("%s %s",
			sql,
			strings.Join(_s.Joins, " "))
	}

	if _s.hasWheres() {
		condition += fmt.Sprintf(" (%s) AND",
			strings.Join(_s.Wheres, " AND "))
	}
	if len(filters) != 0 {
		condition += fmt.Sprintf(" (%s) AND",
			strings.Join(filters, filterType))
	}
	if _s.hasIndex() {
		condition += fmt.Sprintf(" (%s) AND",
			_s.getIndexSQL())
	}

	_len := len(condition)
	if _len > 0 {
		sql += fmt.Sprintf(" WHERE%s", condition[:_len-4])
	}

	return sql
}

func (_s qStruct) composeCountSQL(filterType string, filters []string) (sql string) {
	condition := ""
	sql = fmt.Sprintf("SELECT COUNT(1) FROM `%s`",
		_s.Table)

	if _s.hasJoins() {
		sql = fmt.Sprintf("%s %s",
			sql,
			strings.Join(_s.Joins, " "))
	}

	if _s.hasWheres() {
		condition += fmt.Sprintf(" (%s) AND",
			strings.Join(_s.Wheres, " AND "))
	}
	if len(filters) != 0 {
		condition += fmt.Sprintf(" (%s) AND",
			strings.Join(filters, filterType))
	}
	if _s.hasIndex() {
		condition += fmt.Sprintf(" (%s) AND",
			_s.getIndexSQL())
	}

	_len := len(condition)
	if _len > 0 {
		sql += fmt.Sprintf(" WHERE%s", condition[:_len-4])
	}

	return sql
}

// UPDATE TAB SET <>=<> WHERE Idx;
// TODO: Support joint table update!
func (_s qStruct) composeUpdateSQL(filterType string, filters []string, limit int) (sql string) {
	var (
		updates      []string
		update       string
		colVal       map[string][]string
		key          string
		condition    string
		hasCondition bool
		hasLimit     = limit != 0
	)

	for i := 0; i < _s.Length; i++ {
		update = ""
		condition = ""
		hasCondition = false
		colVal = make(map[string][]string)
		for _, _field := range _s.Fields {
			if _field.IsIndex == false && _field.Table == _s.Table {
				key = fmt.Sprintf("`%s`", _field.ColName)
				if colVal[key] == nil {
					colVal[key] = make([]string, 0)
				}
				if !isEqual(_s.getValueInterface(_field.ValIdx, i), _field.AsNull) {
					if _field.Json != "" {
						colVal[key] = append(colVal[key], fmt.Sprintf("%s', %#v", _field.Json, _s.getValueInterface(_field.ValIdx, i)))
					} else {
						colVal[key] = append(colVal[key], fmt.Sprintf("%#v", _s.getValueInterface(_field.ValIdx, i)))
					}
					// update += fmt.Sprintf(" `%s`=%#v,", _field.ColName, _s.getValueInterface(_field.ValIdx, i))
				} else if _field.Alt != nil && !isEqual(_field.AsNull, _field.Alt) {
					// if AsNull == Alt, ignore!
					if _field.Json != "" {
						colVal[key] = append(colVal[key], fmt.Sprintf("'$.%s', %#v", _field.Json, _field.Alt))
					} else {
						colVal[key] = append(colVal[key], fmt.Sprintf("%#v", _field.Alt))
					}
					// update += fmt.Sprintf(" `%s`=%#v,", _field.ColName, _field.Alt)
				}
			}
		}
		updates = make([]string, 0, len(colVal))

		for _key, _val := range colVal {
			if len(_val) > 0 {
				update = fmt.Sprintf("%s=", _key)
				if len(_val) > 1 {
					update += fmt.Sprintf("IF(JSON_VALID(%s), JSON_SET(%s, '$.%s), JSON_OBJECT('%s))", _key, _key, strings.Join(_val, ", '$."), strings.Join(_val, ", '"))
				} else {
					update += _val[0]
				}
				updates = append(updates, update)
			}
		}

		if _s.hasIndex() {
			hasCondition = true
			for _, _index := range _s.Index {
				condition += fmt.Sprintf(" `%s`=%#v AND", _index.ColName, _s.getValueInterface(_index.ValIdx, i))
			}
		}

		if _s.hasWheres() {
			hasCondition = true
			condition += fmt.Sprintf(" (%s) AND", strings.Join(_s.Wheres, " AND "))
		}

		if len(filters) != 0 {
			hasCondition = true
			condition += fmt.Sprintf(" (%s) AND", strings.Join(filters, filterType))
		}

		if hasCondition {
			sql = fmt.Sprintf("%sUPDATE `%s` SET%s WHERE%s;", sql, _s.Table, strings.Join(updates, ", "), condition[:len(condition)-4])
			if hasLimit {
				sql = fmt.Sprintf("%s LIMIT %#v;", sql[:len(sql)-1], limit)
			}
		} else if hasLimit {
			sql = fmt.Sprintf("%sUPDATE `%s` SET%s LIMIT %#v;", sql, _s.Table, strings.Join(updates, ", "), limit)
		}
	}

	return sql
}

// UPDATE categories
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
func (_s qStruct) composeBatchUpdateSQL() (sql string) {
	var (
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
					fieldName[_i] += fmt.Sprintf("END")
				}
				_i++
			}
		}
	}

	update = strings.Join(fieldName, ", ")

	for _condition := range condMap {
		condition += _condition + ", "
	}

	sql = fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s` IN (%s);", _s.Table, update, indexName, condition[:len(condition)-2])

	return sql
}

func (_s qStruct) composeCreateTableSQL() (sql string) {
	var (
		fields = make([]string, len(_s.Fields))
	)

	for _idx, _values := range _s.Fields {
		fields[_idx] = fmt.Sprintf("`%s` %s", _values.ColName, _values.Schema)
	}

	fieldsDef := strings.Join(fields, ", ")
	indexDef := strings.Join(_s.Schema, ", ")
	if indexDef != "" {
		indexDef = ", " + indexDef
	}

	sql = fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s%s) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;", _s.Table, fieldsDef, indexDef)

	return
}
