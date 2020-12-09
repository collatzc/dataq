package dataq

import "fmt"

type qField struct {
	Table    string
	ColName  string
	AsNull   interface{}
	Alt      interface{}
	Json     string
	JsonCast bool
	Self     string
	Schema   string
	ValIdx   int
	IsIndex  bool
}

func (_f qField) String() string {
	return fmt.Sprintf("\tField: {\n\tTable:\t\t%v\n\tColName:\t%v\n\tAsNull:\t\t%#v\n\tAlt:\t\t%#v\n\tJson:\t\t%v\n\tJsonCast:\t%v\n\tSelf:\t\t%v\n\tSchema:\t\t%v\n\tValIdx:\t\t%#v\n\tIsIndex:\t%v\n}\n",
		_f.Table, _f.ColName, _f.AsNull, _f.Alt, _f.Json, _f.JsonCast, _f.Self, _f.Schema, _f.ValIdx, _f.IsIndex)
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
