package dataq

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/collatzc/jsonl"

	_ "github.com/go-sql-driver/mysql"
)

/**
Test SQL>
SET PASSWORD FOR 'root'@'%' = PASSWORD('112358');
CREATE DATABASE IF NOT EXISTS test;
USE test;
CREATE TABLE IF NOT EXISTS Person (
	ID INT(5) AUTO_INCREMENT,
	NAME VARCHAR(50) DEFAULT '',
	AGE TINYINT(2) DEFAULT 0,
	PROFILE JSON,
	Json JSON,
	CREATED DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (ID)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS Info (
	ID INT(5) AUTO_INCREMENT,
	P_ID INT(5) NOT NULL,
	CMT VARCHAR(50) NULL,
	PRIMARY KEY (ID)
) ENGINE=InnoDB;
delimiter //
CREATE TRIGGER Per_TRIGGER BEFORE INSERT ON Person
	FOR EACH ROW BEGIN
    IF ISNULL(NEW.PROFILE) = 0 OR NEW.PROFILE is NULL THEN
		SET NEW.PROFILE='{}';
	END IF;
END;//
**/

func TestMapWithStringAsKey(t *testing.T) {
	a := make([]map[string]interface{}, 0, 10)
	for i := 0; i < 20; i++ {
		r1 := make(map[string]interface{})
		r1["Field1"] = fmt.Sprintf("Value1 %d", i)
		r1["Field2"] = fmt.Sprintf("Value2 %d", i)
		a = append(a, r1)
	}
	t.Error(len(a))
	for _, item := range a {
		t.Error(item)
		if field, ok := item["Field3"]; ok {
			t.Error("OK", field)
		}
	}
}

func TestVariablesMap(t *testing.T) {
	var a map[string]string
	a = map[string]string{
		"$T0": "abc",
		"$T1": "def",
	}
	t.Error(len(a))
	for _key, _val := range a {
		t.Error(_key, _val)
	}
}

func TestReflectValue(t *testing.T) {
	var a []string
	// GOOD!
	value := reflect.Indirect(reflect.ValueOf(a))
	// slice
	t.Error("Kind", value.Kind())
	t.Error(fmt.Sprintf("%#v", a))
	t.Error("CanSet", value.CanSet())
	t.Error("CanAddr", value.CanAddr())
	t.Error("IsNil", value.IsNil())
	t.Error("Len", value.Len())
	t.Error("Cap", value.Cap())
	t.Error("IsValid", value.IsValid())
	// err: elem not exist
	// t.Error("0", value.Index(0).Field(0))
	// 1. Elem()
	elemType := value.Type().Elem()
	t.Error("TypeName", value.Type().Name())
	newElemValue := reflect.New(elemType)
	newElemValue.Elem().Field(0).SetInt(12)
	newElemValue.Elem().Field(1).SetString("OK")
	value.Set(reflect.Append(value, newElemValue.Elem()))
	t.Error(a, len(a), cap(a))
}

func TestConvertMapToJSON(t *testing.T) {
	config := map[string]interface{}{
		"currentTermName": "2020WS",
		"currentTermGUID": "asdfasdfasdfasfd",
		"todoInt":         2,
	}
	refVal := reflect.ValueOf(config)

	t.Error(refVal.Type().Name())
	t.Errorf("%v", refVal.Kind())
	if refVal.Kind() == reflect.Map {
		t.Error("map yes")
	}
	if fmt.Sprintf("%v", refVal.Type()) != "map[string]interface {}" {
		t.Error("no")
	}

	t.Errorf("%#v", config)
}

func TestAsNull(t *testing.T) {
	var a map[string]interface{}
	var str = "map[string]interface {}(nil)"
	var iii = "1"
	var testTime = time.Time{}
	j, _ := json.Marshal(a)
	t.Errorf("%#v %#v %#v %v", j, a, str, iii)
	t.Errorf("%v", testTime)
}

func assertEqual(t *testing.T, a interface{}, b interface{}, message string) {
	if a == b {
		return
	}
	if len(message) == 0 {
		message = fmt.Sprintf("%v != %v", a, b)
	}
	t.Fatal(message)
}

func getDSN(t *testing.T) string {
	config, err := jsonl.JSONFileObj("./config.json")
	if err != nil {
		t.Fatal("need dsn to test")
	}

	return config.Get("dsn", nil).(string)
}

type Info struct {
	ID       int64  `COL:"ID" TABLE:"Info"`
	PersonID int64  `COL:"P_ID"`
	Comment  string `COL:"CMT"`
}

type Person struct {
	ID      int64     `COL:"ID" TABLE:"Person"`
	Name    string    `COL:"NAME" ALT:""`
	Age     int       `COL:"AGE"`
	Profile string    `COL:"PROFILE" ALT:"{}" WHERE:"PROFILE<>\"\""`
	Created time.Time `COL:"CREATED"`
	Omit    string    `OMIT:""`
}

type Person1 struct {
	ID   int64  `INDEX:"" COL:"ID" TABLE:"Person"`
	Name string `COL:"NAME" ALT:""`
	Age  int    `COL:"AGE"`
}

type Person2 struct {
	ID   int64  `INDEX:"" COL:"ID" TABLE:"Person"`
	Name string `COL:"NAME" ALT:""`
	Age  int    `COL:"AGE"`
}

type PersonJson struct {
	ID       int64                  `INDEX:"" COL:"ID" TABLE:"Person"`
	Name     string                 `COL:"NAME" ALT:""`
	Age      int                    `COL:"AGE" SELF:"+1"`
	Json     map[string]interface{} `JSONMERGEPATCH:"Json"`
	Password string                 `JSON:"PROFILE.password" json:"password"`
	Username string                 `JSON:"PROFILE.userName" json:"username"`
	Created  time.Time              `COL:"CREATED"`
	Omit     string                 `OMIT:""`
}

type PersonNoTable struct {
	ID      int64     `COL:"ID" SCHEMAF:"int unsigned AUTO_INCREMENT" SCHEMAT:"PRIMARY KEY(ID)"`
	Name    string    `COL:"NAME" ALT:"" SCHEMAF:"varchar(20) DEFAULT ''"`
	Age     int       `COL:"AGE" SCHEMAF:"tinyint DEFAULT '0'"`
	Profile string    `COL:"PROFILE" ALT:"{}" SCHEMAF:"JSON"`
	Created time.Time `COL:"CREATED" SCHEMAF:"datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"`
	Omit    string    `OMIT:""`
}

type CurrTimestamp struct {
	Timestamp string `COL:"CURRENT_TIMESTAMP" NOFROM:""`
	Date      string `COL:"CURDATE()" NOTABLE:""`
}

/**
 * `ASNULL` the value of the field equal ASNULL will be ignore in INSERT-statement
 */
type PersonInfo struct {
	PId     int64                  `INDEX:"" COL:"Person.ID" TABLE:"Person" JOIN:"JOIN Info ON Info.P_ID=Person.ID" WHERE:"Person.ID=1"`
	Name    string                 `COL:"Info.Name"`
	Json    map[string]interface{} `JSONPATCHMERGE:"Json"`
	Age     int                    `INDEX:"Art"`
	Profile string                 `ASNULL:"" ALT:"{}"`
	Comment string                 `COL:"CMT"`
}

func TestComposeSQL(t *testing.T) {
	p := PersonJson{
		ID: 3,
		Json: map[string]interface{}{
			"a": 1,
			"b": 2,
		},
		Name:     "CC",
		Password: "password123123123",
		Username: "username123123123",
	}
	stuP, _ := analyseStruct(p)
	insertSql := stuP.composeInsertSQL()
	t.Error(insertSql)
	updateSql := stuP.composeUpdateSQL([]qClause{{"AND", "`NAME`=?", []interface{}{"123"}}, {"OR", "`AGE`=?", []interface{}{1}}}, 0)
	t.Error(updateSql)
	t.Error("Values", stuP.GetValues())
	selectSql := stuP.composeSelectSQL(nil)
	t.Error(selectSql)
	pp := []PersonJson{
		{
			ID:       1,
			Name:     "",
			Age:      4,
			Password: "password123123123",
		}, {
			ID:       2,
			Name:     "BB",
			Age:      3,
			Password: "password123123123",
		},
	}
	strPp, _ := analyseStruct(&pp)
	mInsertSql := strPp.composeInsertSQL()
	t.Error(mInsertSql)
	t.Errorf("Values: %#v", strPp.GetValues())
	mUpdateSql := strPp.composeUpdateSQL([]qClause{{"AND", "`NAME`=?", []interface{}{"123"}}}, 0)
	t.Error(mUpdateSql)
	t.Errorf("Values: %#v", strPp.GetValues())
	mSelectSql := strPp.composeSelectSQL(nil)
	t.Error(mSelectSql)
	t.Errorf("Values: %#v", strPp.GetValues())
}

func TestBatchInsertSQL(t *testing.T) {
	p := PersonInfo{
		Name: "CC",
		Age:  12,
	}
	stuP, _ := analyseStruct(p)
	t.Error("IsBatchValueEmpty: ", stuP.IsBatchValueEmpty())
	v11 := make(map[string]interface{})
	v11["Name"] = "DD"
	v11["Age"] = 13
	t.Error(v11)
	v12 := make(map[string]interface{})
	v12["Name"] = "EE"
	v12["Age"] = 14
	stuP.OnDuplicateKeyUpdate = true
	duplicateKeyUpdateCol := make(map[string]interface{})
	duplicateKeyUpdateCol["Name"] = "VALUES(`Name`)"
	duplicateKeyUpdateCol["Age"] = "`Age` + VALUES(`Age`)"
	stuP.DuplicateKeyUpdateCol = duplicateKeyUpdateCol
	stuP.AppendBatchValue(v11)
	stuP.AppendBatchValue(v12)
	t.Error("After append, empty? ", stuP.IsBatchValueEmpty())

	t.Error(stuP.composeBatchInsertSQL())
}

func TestBatchUpdateSQL(t *testing.T) {
	p := PersonInfo{
		Name: "CC",
		Age:  12,
	}
	stuP, _ := analyseStruct(p)
	t.Error("IsBatchValueEmpty: ", stuP.IsBatchValueEmpty())
	v11 := make(map[string]interface{})
	v11["INDEX"] = 1
	v11["Age"] = 20
	v11["Name"] = "Hu"
	t.Error(v11)
	v12 := make(map[string]interface{})
	v12["INDEX"] = 2
	v12["Age"] = 27
	v12["Name"] = "Chen"
	stuP.AppendBatchValue(v11)
	stuP.AppendBatchValue(v12)
	t.Error("After append, empty? ", stuP.IsBatchValueEmpty())

	t.Error(stuP.composeBatchUpdateSQL())
}

func TestBatchInsert(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	checkErr(err, t)
	defer db.Close()

	per := Person{}
	model := db.Model(per)
	model.SetBatchMode(true)
	v1 := map[string]interface{}{
		"NAME": "Sun",
		"AGE":  100,
	}
	model.AppendBatchValue(v1)
	v2 := map[string]interface{}{
		"NAME": "Moon",
		"AGE":  99,
	}
	model.AppendBatchValue(v2)
	t.Error(model.BatchInsert())
}

func TestBatchUpdate(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	checkErr(err, t)
	defer db.Close()

	per := Person{}
	model := db.Model(per)
	model.SetBatchMode(true)
	v1 := map[string]interface{}{
		"INDEX": 8,
		"NAME":  "Sun",
		"AGE":   100,
	}
	model.AppendBatchValue(v1)
	v2 := map[string]interface{}{
		"INDEX": 7,
		"NAME":  "Moon",
		"AGE":   99,
	}
	model.AppendBatchValue(v2)
	t.Error(model.BatchUpdate())
}

// Note: From general_log shows that each routine will start a new connection with MySQL
// but only the first connection will run SELECT DATABASE().
func TestMultiTransactionWithPreparedStmt(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	checkErr(err, t)
	defer db.Close()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			tx := db.Begin()
			defer func() {
				if i%2 != 0 {
					tx.Commit()
					t.Errorf("%d Committed", i)
				} else {
					tx.Rollback()
					t.Errorf("%d Rollback", i)
				}
				wg.Done()
			}()
			p := Person{

				Name: fmt.Sprintf("No %d", i),
				Age:  i + 10,
			}
			p1 := Person1{
				Name: fmt.Sprintf("No %d", i),
				Age:  i + 10,
			}
			p2 := Person2{
				Name: fmt.Sprintf("No %d", i),
				Age:  i + 10,
			}
			t.Error(tx.Model(p).PrepareNext(true).Insert())
			t.Error(tx.Model(p1).PrepareNext(true).Insert())
			t.Error(tx.Model(p2).PrepareNext(true).Insert())
			t.Error(tx.Model(p).PrepareNext(true).Insert())
			t.Error(tx.Model(p1).PrepareNext(true).Insert())
			t.Error(tx.Model(p2).PrepareNext(true).Insert())
		}(i)
	}

	wg.Wait()
}

func TestMultiInsert(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	checkErr(err, t)
	defer db.Close()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer func() {
				t.Error(i)
				wg.Done()
			}()

			p := Person{
				Name: fmt.Sprintf("No %d", i),
				Age:  i + 10,
			}
			p1 := Person1{
				Name: fmt.Sprintf("No %d", i),
				Age:  i + 10,
			}
			p2 := Person2{
				Name: fmt.Sprintf("No %d", i),
				Age:  i + 10,
			}
			db.Model(p).Insert()
			db.Model(p1).Insert()
			db.Model(p2).Insert()
			db.Model(p).Insert()
			db.Model(p1).Insert()
			db.Model(p2).Insert()
		}(i)
	}

	wg.Wait()
}

func TestColFunc(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	checkErr(err, t)
	defer db.Close()

	ts := new(CurrTimestamp)
	// ts := &CurrTimestamp{}
	// ts := CurrTimestamp{}
	antQ := db.Model(ts).Query()
	t.Error(antQ)
	t.Error(ts)
}

func TestInsert(t *testing.T) {
	db, err := Open(getDSN(t), 3)
	checkErr(err, t)
	defer db.Close()

	t.Error("DBNAME:", db.DBName())

	per := []Person{
		{
			Name: "P1",
			Age:  1,
		}, {
			Name: "P2",
			Age:  2,
		},
	}

	// per1 := Person{
	// 	Name: "AA",
	// 	Age:  4,
	// }

	model := db.Model(&per)

	t.Error(model.Insert())
}

func checkErr(err error, t *testing.T) {
	if err != nil {
		t.Error(err)
	}
}

func TestTimeTime(t *testing.T) {
	type testTime struct {
		CreatedAt time.Time `json:"createdAt"`
	}
	// using Date.prototype.toISOString() to get
	var str = []byte(`{"createdAt":"2020-10-15T17:53:57.887Z"}`)
	var a testTime
	err := json.Unmarshal(str, &a)
	if err != nil {
		t.Fatal(err)
	}
	av := reflect.ValueOf(a)
	t.Errorf("%#v", av.Field(0).Interface().(time.Time).Format(ConfigMySQLDateTimeFormat))
}

func TestRawSQL(t *testing.T) {
	db, err := sql.Open("mysql", getDSN(t))
	checkErr(err, t)

	tx, err := db.Begin()
	checkErr(err, t)
	defer tx.Commit()
	// t.Error(result.RowsAffected())

	stmt, err := tx.Prepare("INSERT INTO `Person` (`ID`, `NAME`, `AGE`) VALUES (?, ?, ?),(?, ?, ?);")
	checkErr(err, t)

	var vals = []interface{}{13, "Tx with prepare", 5, 14, "TxOK", 6}
	res, err := tx.Stmt(stmt).Exec(vals...)
	// checkErr(err, t)
	id, err := res.LastInsertId()
	// checkErr(err, t)

	t.Error("Prepared: ", id)
}

func TestDynamicTableName(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	checkErr(err, t)
	defer db.Close()

	p := make([]PersonNoTable, 2)
	m := db.Model(p)
	// Set the tablename @runtime
	m.Table("Person")
	res := m.Query()
	t.Error(res)
	t.Error(p)
}

func TestCreateTable(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	checkErr(err, t)
	defer db.Close()

	p := PersonNoTable{}
	m := db.Model(p)
	m.TableSchema("KEY `kName` (`NAME`)",
		"KEY `kAge` (`AGE`)")

	m.Table("Person2")
	res := m.CreateTable()
	t.Error(res)
}

func TestUpdate(t *testing.T) {
	db, err := Open(getDSN(t), 3)
	checkErr(err, t)
	defer db.Close()

	per1 := []Person{
		{
			ID:  11,
			Age: 6,
		},
		{
			ID:  12,
			Age: 8,
		},
	}
	m := db.Model(per1)
	t.Errorf("%#v\n", m.Update())
	res := m.Query()
	t.Errorf("%#v\n", res.Error)
	t.Errorf("%#v\n", per1)
}

func TestQuery(t *testing.T) {
	ConfigParseDateTimeFormat = "2006-01-02 15:04:05"
	db, err := Open(getDSN(t), 2)
	checkErr(err, t)
	defer db.Close()
	// per1 := make([]Person, 0)
	per1 := []Person{}
	// per1 := []Person{
	//   {
	//     ID: 12,
	//   },
	//   {
	//     ID: 14,
	//   },
	// }
	// t.Errorf("%#v\n", db.Q().Where("ID>3").Models(&per1))
	t.Errorf("%#v\n", db.Model(&per1).Query())
	t.Error(per1)
}
