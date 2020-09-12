package dataq

import "fmt"

type qField struct {
	// Table name
	Table string
	// Field name in DB or a function
	ColName string
	// This value will be interpreted as NULL
	AsNull interface{}
	// Alternative
	Alt interface{}
	// Element from a JSON datatype
	Json string
	// Schema for `CREATE TABLE`
	Schema  string
	ValIdx  int
	IsIndex bool
}

func (_f qField) String() string {
	return fmt.Sprintf("Query Field: {\n\tTable:\t\t%v\n\tColName:\t%v\n\tAsNull:\t\t%#v\n\tAlt:\t\t%#v\n\tSchema:\t\t%v\n\tValIdx:\t\t%#v\n\tIsIndex:\t%v\n}\n",
		_f.Table, _f.ColName, _f.AsNull, _f.Alt, _f.Schema, _f.ValIdx, _f.IsIndex)
}

func (_f qField) SelectString() (field string) {
	if len(_f.Table) != 0 {
		field = fmt.Sprintf("`%s`.`%s`", _f.Table, _f.ColName)

		if _f.Json != "" {
			field = fmt.Sprintf("%s->>'$.%s'", field, _f.Json)
		}
	} else {
		field = fmt.Sprintf("%s", _f.ColName)
	}

	return
}
