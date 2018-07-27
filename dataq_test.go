package dataq

import (
	"fmt"
	"testing"

	"github.com/collatzc/jsonl"

	_ "github.com/go-sql-driver/mysql"
)

/**
Test SQL>
SET PASSWORD FOR 'root'@'%' = PASSWRD('112358');
CREATE DATABASE IF NOT EXISTS test;
USE test;
CREATE TABLE IF NOT EXISTS Person (
	ID INT(5) AUTO_INCREMENT,
	NAME VARCHAR(50) DEFAULT '',
	AGE TINYINT(2) DEFAULT 0,
	PROFILE JSON,
	CREATED DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (ID)
) ENGINE=MyISAM;
CREATE TABLE IF NOT EXISTS Info (
	ID INT(5) AUTO_INCREMENT,
	P_ID INT(5) NOT NULL,
	CMT VARCHAR(50) NULL,
	PRIMARY KEY (ID)
) ENGINE=MyISAM;
**/

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
	ID      int64  `PK:"true" COL:"ID" TABLE:"Person"`
	Name    string `COL:"NAME"`
	Age     int    `COL:"AGE"`
	Profile string `COL:"PROFILE"`
}

type PersonInfo struct {
	PId     int64 `PK:"true" COL:"Person.ID" TABLE:"Person" JOIN:"JOIN Info ON Info.P_ID=Person.ID" WHERE:"Person.ID=1"`
	Name    string
	Age     int
	Comment string `COL:"CMT"`
}

func TestInsert(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	if err != nil {
		t.Errorf(err.Error())
	}
	defer db.Close()

	t.Error("DBNAME:", db.DBName())

	per := []Person{Person{
		Name: "",
		Age:  4,
		// Profile: "{}",
	}, Person{
		Name: "BB",
		Age:  3,
		// Profile: "{}",
	}}

	t.Error(db.C().Models(&per))
}

func TestQuery(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	if err != nil {
		t.Errorf(err.Error())
	}
	defer db.Close()
	per1 := make([]Person, 2)
	t.Errorf("%#v\n", db.Q().Models(&per1))
	t.Errorf("%#v\n", per1)
}

func TestInsInfo(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	if err != nil {
		t.Errorf(err.Error())
	}
	defer db.Close()

	/*info1 := Info{
		PersonId: 1,
		Comment:  "Test1",
	}*/
	/* per := []Person{Person{
		ID:   1,
		Name: "Collatz",
		Age:  12,
	}, Person{
		ID:   2,
		Name: "CC",
		Age:  3,
	}} */
	// fmt.Println(db.U().Models(&per))
}

func TestModelToSQL(t *testing.T) {
	db, err := Open(getDSN(t), 2)
	if err != nil {
		t.Errorf(err.Error())
	}
	defer db.Close()

	/*var person Person
	if err = db.Q().Where("ID=1").Model(&person); err != nil {
		t.Errorf(err.Error())
	}
	fmt.Println(person)*/

	/*var pi PersonInfo
	err = db.Q().Model(&pi)
	if err != nil {
		fmt.Println("No rows are found.")
		t.Errorf(err.Error())
		return
	}
	fmt.Println(pi)

	var info []Info
	info = make([]Info, 3)
	err = db.Q().Models(&info)
	if err != nil {
		t.Errorf(err.Error())
	}
	fmt.Println(info)*/
}
