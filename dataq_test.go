package dataq_test

import (
	"testing"
	"time"

	"github.com/collatzc/dataq"
	"github.com/collatzc/jsonl"
	_ "github.com/go-sql-driver/mysql"
)

// CREATE TABLE IF NOT EXISTS Person (
// 	ID INT(5) AUTO_INCREMENT,
// 	NAME VARCHAR(50) DEFAULT '',
// 	AGE TINYINT(2) DEFAULT 0,
// 	PROFILE JSON,
// 	Json JSON,
// 	CREATED DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
// 	PRIMARY KEY (ID)
// ) ENGINE=InnoDB;

type Person struct {
	ID      int64          `COL:"ID" TABLE:"Person"`
	Name    dataq.QString  `COL:"NAME" ALT:""`
	Age     int            `COL:"AGE"`
	Profile string         `COL:"PROFILE" ALT:"{}" JSONARRAYAPPEND:"Profile, '$'"`
	Log     []string       `JSON:"Json.log" JSONMERGEPRESERVE:"Json->>'$.log'" JSONCAST:"" INIT:"[]"`
	Array   dataq.QStrings `JSON:"Json.array" JSONCAST:""`
	QString dataq.QString  `JSON:"Json.qString"`
	Created time.Time      `COL:"CREATED"`
	Omit    string         `OMIT:""`
}

func getDSN(t *testing.T) string {
	config, err := jsonl.JSONFileObj("./config.json")
	if err != nil {
		t.Fatal("need dsn to test")
	}
	return config.Get("dsn", nil).(string)
}

func checkErr(err error, t *testing.T) {
	if err != nil {
		t.Error(err)
	}
}

func TestJsonArrayAppend(t *testing.T) {
	dsn := getDSN(t)
	t.Log("dsn", dsn)
	db, err := dataq.Open(dsn, 5)
	checkErr(err, t)
	defer db.Close()

	tx := db.Begin()
	defer tx.FinDefaultCommit()

	per := Person{
		ID:      3,
		Age:     12,
		Profile: "[\"Log1\"]",
		Array:   dataq.InitQStrings([]string{"A", "C"}),
	}
	sqlRes := tx.Model(per).Insert()
	err = sqlRes.Error
	t.Log(err)

	// per.Profile = "Log4"
	// per.Log = []string{"Log3"}
	// per.Array = dataq.InitQStrings([]string{})
	// t.Fatal("Update()", tx.Model(per).IndexWith(0).Update())

}
