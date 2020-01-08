package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"database/sql"
	"database/sql/driver"
	sf "github.com/snowflakedb/gosnowflake"
)

// TODO free up all c objs esp CString
// TODO figure out if I need to free up the C.char parms
// TODO Close the query (I think it's a noop but that'd be a good place to free up CString)

// Lazy coding: storing last error and connection as global vars bc don't want to figure out how to pkg and pass them
// back and forth to ruby
var last_error error
var db driver.Conn

// export LastError
func LastError() *C.char {
	if last_error == nil {
		return nil
	} else {
		return C.CString(last_error.Error())
	}
}

// @returns nil if no error or the error string
// export Connect
func Connect(account *C.char, warehouse *C.char, database *C.char, schema *C.char,
	user *C.char, password *C.char, role *C.char, port *C.char) *C.char {
	// other optional parms: Application, Host, and alt auth schemes
	cfg := &sf.Config{
		Account:   C.GoString(account),
		Warehouse: C.GoString(warehouse),
		Database:  C.GoString(database),
		Schema:    C.GoString(schema),
		User:      C.GoString(user),
		Password:  C.GoString(password),
		Role:      C.GoString(role),
		Region:    "us-east-1",
		Port:      C.GoString(portStr),
	}
	dsn, last_error := sf.DSN(cfg)
	if last_error != nil {
		return LastError()
	}
	db, last_error = sql.Open("snowflake", dsn)
	if last_error != nil {
		return LastError()
	}
	return nil
}

// export Close
func Close() {
	if db != nil {
		db.Close()
	}
}

// export Exec
func Exec(statement *C.char) int64 {
	var res Result

	res, last_error = db.Exec(C.GoString(statement))
	if res != nil {
		return res.RowsAffected()
	}
	return nil
}

// export Fetch
func Fetch(statement *C.char) *snowflakeRows {

	rows, last_error := db.Query(C.GoString(statement))
	return rows
}

//export Next
func Next(rows *snowflakeRows) []*C.char {
	data, last_error := rows.ChunkDownloader.Next()

	// includes io.EOF
	if last_error == io.EOF {
		rows.ChunkDownloader.Chunks = nil // detach all chunks. No way to go backward without reinitialize it.
	}

	if data != nil {
		result := [len(data)]*C.char

		for i = 0; i < len(data); i++ {
			// TODO figure out if I need to handle db NULL differently
			result[i] = C.CString(data[i])
		}
		return &result[0]
	}
	return nil
}
