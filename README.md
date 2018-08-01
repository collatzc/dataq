# dataq(uelle)
> A light-weight SQL builder for Go

[![Telegram](https://img.shields.io/badge/chat-telegram-blue.svg)](https://t.me/helloblackboard)

## Features

### Mapping `struct` with "Query Entity"

* To execute query == manipulate with struct

	The table definition:
	```SQL
	CREATE TABLE IF NOT EXISTS Person (
		ID INT(5) AUTO_INCREMENT,
		NAME VARCHAR(50) DEFAULT '',
		AGE TINYINT(2) DEFAULT 0,
		PROFILE JSON,
		CREATED DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (ID)
	) ENGINE=InnoDB;
	```
	
	The `struct` definition:
	```golang
	type Person struct {
		ID      int64  `INDEX:"" COL:"ID" TABLE:"Person"`
		Name    string `COL:"NAME" ALT:""`
		Age     int    `COL:"AGE"`
		Profile string `COL:"PROFILE" ALT:"{}" WHERE:"PROFILE<>\"\""`
	}
	```
	Init the connection of database:
	```golang
	db, err := Open("<dns here>")
	checkErr(err, t)
	defer db.Close()
	```
	Init the struct with data to `INSERT`:
	```golang
	p1 := Persion{
		Name: "Mike",
		Age: 18,
	}
	
	model := db.Model(p1)
	resInsert := model.Insert()
	```

* Fix "where condition" or "join condition" in `struct`'s Tag
* Support dynamic "where condition"

## Mini Doc

### Attention

* 
### Tags

* `TABLE` - The table name.
* `COL` - The field name, also can be a function and with Tag `RAW`.
* `INDEX` - This field is an index.
* `ASNULL` - As NULL value.
* `ALT` - Alternative value.
* `WHERE` - The fixed part of `WHERE` clause.
* `JOIN` - The fixed part of `JOIN` clause.
* `OMIT` - This field will be ignored in query.
* `NOFROM` - [Query only!] No `FROM` clause will be generated.
* `RAW` - [Query only!] Will query with what the Tag `COL` has.
