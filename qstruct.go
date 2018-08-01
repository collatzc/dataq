package dataq

import (
	"fmt"
	"reflect"
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
	Wheres    []string
	Value     *reflect.Value
	QueryOnly bool
}

func (_s qStruct) String() string {
	return fmt.Sprintf("Query Struct: {\nTable:\t\t%v\nLength:\t\t%v\nIndex:\t\t%v\nFields:\t\t%v\n\tJoins:\t%v\n\tWheres:\t%v\n\tQueryOnly:\t%v\n}\n", _s.Table, _s.Length, _s.Index, _s.Fields, _s.Joins, _s.Wheres, _s.QueryOnly)
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
	var typeName string
	if _s.Value.Kind() != reflect.Slice {
		typeName = _s.Value.Field(idxField).Type().Name()
		ret = _s.Value.Field(idxField).Interface()
	} else {
		typeName = _s.Value.Index(idxArray).Field(idxField).Type().Name()
		ret = _s.Value.Index(idxArray).Field(idxField).Interface()
	}
	switch typeName {
	case "Time":
		return ret.(time.Time).Format(DateTimeFormat)
	default:
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
		col string
		val string
	)
	for i := 0; i < _s.Length; i++ {
		col = ""
		val = ""
		for _, _field := range _s.Fields {
			// will ignore `TABLE`. prefix
			if !isEqual(_s.getValueInterface(_field.ValIdx, i), _field.AsNull) {
				col += fmt.Sprintf(" `%s`,", _field.ColName)
				val += fmt.Sprintf(" %#v,", _s.getValueInterface(_field.ValIdx, i))
			}
		}
		sql = fmt.Sprintf("%sINSERT INTO `%s` (%s) VALUES (%s);", sql, _s.Table, col[1:len(col)-1], val[1:len(val)-1])
	}

	return sql
}

func (_s qStruct) composeSelectFieldSQL() (sql string) {
	for _, _field := range _s.Fields {
		if _s.QueryOnly == false {
			sql += fmt.Sprintf(" `%s`.`%s`,",
				_field.Table,
				_field.ColName)
		} else {
			sql += fmt.Sprintf(" %s,", _field.ColName)
		}
	}

	return sql[1 : len(sql)-1]
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
		update       string
		condition    string
		hasCondition bool
		hasLimit     = limit != 0
	)

	for i := 0; i < _s.Length; i++ {
		update = ""
		condition = ""
		hasCondition = false
		for _, _field := range _s.Fields {
			if _field.IsIndex == false && _field.Table == _s.Table {
				if !isEqual(_s.getValueInterface(_field.ValIdx, i), _field.AsNull) {
					update += fmt.Sprintf(" `%s`=%#v,", _field.ColName, _s.getValueInterface(_field.ValIdx, i))
				} else if _field.Alt != nil && !isEqual(_field.AsNull, _field.Alt) {
					update += fmt.Sprintf(" `%s`=%#v,", _field.ColName, _field.Alt)
				}
				// if AsNull == Alt, ignore!
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
		fmt.Println(update)
		if hasCondition {
			sql = fmt.Sprintf("%sUPDATE `%s` SET%s WHERE%s;", sql, _s.Table, update[:len(update)-1], condition[:len(condition)-4])
			if hasLimit {
				sql = fmt.Sprintf("%s LIMIT %#v;", sql[:len(sql)-1], limit)
			}
		} else if hasLimit {
			sql = fmt.Sprintf("%sUPDATE `%s` SET%s LIMIT %#v;", sql, _s.Table, update[:len(update)-1], limit)
		}
	}

	return sql
}
