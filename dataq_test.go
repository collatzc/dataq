package dataq_test

import (
	"testing"
	"time"

	"github.com/collatzc/dataq"
	"github.com/collatzc/jsonl"
	_ "github.com/go-sql-driver/mysql"
)

type Person struct {
	ID      int64     `COL:"ID" TABLE:"Person"`
	Name    string    `COL:"NAME" ALT:""`
	Age     int       `COL:"AGE"`
	Profile string    `COL:"PROFILE" ALT:"{}" JSONARRAYAPPEND:"Profile, '$'"`
	Log     []string  `JSON:"Json.log" JSONMERGEPRESERVE:"Json->>'$.log'" JSONCAST:"" INIT:"[]"`
	Array   []string  `JSON:"Json.array" JSONCAST:""`
	Created time.Time `COL:"CREATED"`
	Omit    string    `OMIT:""`
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
	db, err := dataq.Open(dsn, 1)
	checkErr(err, t)
	defer db.Close()

	tx := db.Begin()
	defer tx.FinDefaultCommit()

	per := Person{
		ID:      14,
		Name:    "P1",
		Age:     12,
		Array:   []string{"A", "B"},
		Profile: "[\"Log1\"]",
	}
	sqlRes := tx.Model(per).Insert()
	err = sqlRes.Error
	t.Log(err)

	per.Profile = "Log4"
	per.Log = []string{"Log3"}
	per.Array = []string{}
	t.Fatal("Update()", tx.Model(per).IndexWith(0).Update())

}
