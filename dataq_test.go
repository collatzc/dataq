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
	Log     []string  `JSON:"Json.log" JSONMERGEPRESERVE:"Json->>'$.log'" INIT:""`
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

	per := Person{
		ID:      2,
		Name:    "P1",
		Age:     12,
		Profile: "[\"Log1\"]",
	}
	t.Log("Inert()", db.Model(per).Insert())

	per.Profile = "Log5"
	per.Log = []string{"Log6"}
	t.Error("Update()", db.Model(per).IndexWith(0).Update())
}
