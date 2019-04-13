package dataq

import (
	"database/sql"
	"fmt"
	"reflect"
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

func TestSliceEmpty(t *testing.T) {

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
	ID      int64     `INDEX:"" COL:"ID" TABLE:"Person"`
	Name    string    `COL:"NAME" ALT:""`
	Age     int       `COL:"AGE"`
	Profile string    `COL:"PROFILE" ALT:"{}" WHERE:"PROFILE<>\"\""`
	Created time.Time `COL:"CREATED"`
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
	PId     int64  `INDEX:"" COL:"Person.ID" TABLE:"Person" JOIN:"JOIN Info ON Info.P_ID=Person.ID" WHERE:"Person.ID=1"`
	Name    string `COL:"Info.Name"`
	Age     int    `INDEX:"Art"`
	Profile string `ASNULL:"" ALT:"{}"`
	Comment string `COL:"CMT"`
}

func TestComposeInsertSQL(t *testing.T) {
	p := PersonInfo{
		Name: "CC",
		Age:  12,
	}
	stuP, _ := analyseStruct(p)
	t.Error(stuP.composeInsertSQL())
	t.Error(stuP.composeUpdateSQL("", nil, 0))
	t.Error(stuP.composeSelectSQL(" OR ", nil))
	pp := []Person{Person{
		Name: "",
		Age:  4,
	}, Person{
		Name:    "BB",
		Age:     3,
		Profile: "{}",
	}}
	strPp, _ := analyseStruct(&pp)
	t.Error(strPp.composeSelectSQL(" AND ", nil))
	t.Error(strPp.composeInsertSQL())
	t.Error(strPp.composeUpdateSQL("", nil, 0))
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
	db, err := Open(getDSN(t), 2)
	checkErr(err, t)
	defer db.Close()

	t.Error("DBNAME:", db.DBName())

	per := []Person{Person{
		Name: "P5",
		Age:  3,
	}, Person{
		Name: "P6",
		Age:  4,
	}}

	// per1 := Person{
	// 	Name: "AA",
	// 	Age:  4,
	// }

	model := db.Model(&per).Begin()
	defer model.Commit()

	t.Error(model.Insert())
}

func checkErr(err error, t *testing.T) {
	if err != nil {
		t.Error(err)
	}
}

func TestTimeTime(t *testing.T) {
	var a time.Time
	av := reflect.ValueOf(a)
	t.Errorf("%#v", av.Interface().(time.Time).Format(time.RFC3339))
}

func TestRawSQL(t *testing.T) {
	db, err := sql.Open("mysql", getDSN(t))
	checkErr(err, t)

	tx, err := db.Begin()
	checkErr(err, t)
	defer tx.Rollback()

	result, err := tx.Exec("INSERT INTO `Person` (`ID`, `NAME`, `AGE`, `PROFILE`) VALUES (0, \"AA\", 4, \"\")")
	fmt.Println("result:", result)
	// t.Error("Should have a panic")
	// checkErr(err, t)

	// t.Error(result.RowsAffected())

	stmt, err := tx.Prepare("INSERT INTO `Person` (`ID`, `NAME`, `AGE`) VALUES (?, ?, ?)")
	// checkErr(err, t)

	res, err := tx.Stmt(stmt).Exec(0, "TX with prepare", 5)
	// checkErr(err, t)
	id, err := res.LastInsertId()
	// checkErr(err, t)

	t.Error("Prepared: ", id)
}

func TestUpdate(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	checkErr(err, t)
	defer db.Close()

	per1 := []Person{
		Person{
			ID:  2,
			Age: 6,
		},
		Person{
			ID:  4,
			Age: 8,
		},
	}
	m := db.Model(per1)
	// t.Errorf("%#v\n", m.Update())
	res := m.Query()
	t.Errorf("%#v\n", res.Error)
	t.Errorf("%#v\n", per1)
	t.Errorf("%v\n", per1)
}

func TestQuery(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	checkErr(err, t)
	defer db.Close()
	// per1 := make([]Person, 2)
	per1 := []Person{
		Person{
			ID: 2,
		},
		Person{
			ID: 4,
		},
	}
	// t.Errorf("%#v\n", db.Q().Where("ID>3").Models(&per1))
	t.Errorf("%#v\n", db.Model(per1).Count())
	t.Errorf("%#v\n", per1)
}
