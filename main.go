package main

/*
#include <stdlib.h>
*/
import "C"
import (
    "C"
    "unsafe"
    "database/sql"
	"database/sql/driver"
    sf "github.com/snowflakedb/gosnowflake"
)

// TODO free up all c objs esp CString
// TODO figure out if I need to free up the C.char parms
// TODO Close the query (I think it's a noop but that'd be a good place to free up CString)

// do i need to xport these types?
type connection struct {
    db   driver.Conn
    err  *C.char
}

type statementResult struct {
    rowsAffected    int64
    err             *C.char
}

// export query
type Query struct {
    rows *snowflakeRows
    err  *C.char
}

type row struct {
    err     *C.char
    values  []*C.char
}

// export Connect
func Connect(account *C.char, warehouse *C.char, database *C.char, schema *C.char,
            user *C.char, password *C.char, role *C.char, port *C.char) *connection {
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
    dsn, err := sf.DSN(cfg)
    if err != nil {
        return connection{err: C.CString(err.Error())}
    }
	db, err := sql.Open("snowflake", dsn)
    if err != nil {
        return connection{err: C.CString(err.Error())}
    }
	return &connection{db: db}
}

// export Close
func Close(conn *connection) {
    if conn.db != nil {
        conn.db.Close()
    }
    if conn.err != nil {
        C.free(unsafe.Pointer(conn.err))
    }
}

// export Exec
func Exec(conn *connection, statement *C.char) *statementResult {
    var res Result
	var err error
    var result statementResult

    res, err = conn.db.Exec(C.GoString(statement))
    if res != nil {
        result.rowsAffected = res.RowsAffected()
    }
    if err != nil {
        result.err = C.CString(err.Error())
    }
    return &result
}

// export Fetch
func Fetch(conn *connection, statement *C.char) *Query {

    rows, err = conn.db.Query(C.GoString(statement))
    result := Query{rows: rows}

    if err != nil {
        result.err = C.CString(err.Error())
    }
    return &result
}

//export Next
func Next(queryStruct *Query) *row {
    data, err := queryStruct.rows.ChunkDownloader.Next()
    //dataLength := len(data)

    // TODO fixme so we set the array length
    result := row{}
    //result.values = [dataLength]*C.char
    //result.values = [4]*C.char

    if err != nil {
        result.err = C.CString(err.Error())
        // includes io.EOF
        if err == io.EOF {
            rows.ChunkDownloader.Chunks = nil // detach all chunks. No way to go backward without reinitialize it.
        }
    }

    for i = 0; i < len(data); i++ {
        // TODO figure out if I need to handle db NULL differently
        result.values[i] = C.CString(data[i])
    }
    return &result
}

