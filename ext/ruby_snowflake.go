package main

/*
#include <stdlib.h>
*/
import "C"
import (
  "database/sql"
  "errors"
  sf "github.com/snowflakedb/gosnowflake"
  "unsafe"
  "io"
  gopointer "github.com/mattn/go-pointer"
//  "fmt"
)

// TODO free up all c objs esp CString
// TODO Close the query (I think it's a noop tho)

// Lazy coding: storing last error and connection as global vars bc don't want to figure out how to pkg and pass them
// back and forth to ruby
var last_error error
var db *sql.DB  // TODO follow gopointer pattern to return this to ruby

//export LastError
func LastError() *C.char {
  if last_error == nil {
    return nil
  } else {
    return C.CString(last_error.Error())
  }
}

// @returns nil if no error or the error string
// ugh, ruby and go were disagreeing about the length of `int` so I had to be particular here and in the ffi
//export Connect
func Connect(account *C.char, warehouse *C.char, database *C.char, schema *C.char,
  user *C.char, password *C.char, role *C.char, port int64) *C.char {
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
    Port:      int(port),
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

//export Close
func Close() {
  if db != nil {
    db.Close()
  }
}

// @return number of rows affected or -1 for error
//export Exec
func Exec(statement *C.char) int64 {
  var res sql.Result
  res, last_error = db.Exec(C.GoString(statement))
  if res != nil {
    rows, _ := res.RowsAffected()
    return rows
  }
  return -1
}

//export Fetch
func Fetch(statement *C.char) unsafe.Pointer {
  var rows *sql.Rows
  rows, last_error = db.Query(C.GoString(statement))
  if rows != nil {
    result := gopointer.Save(rows)
    return result
  } else {
    return nil
  }
}

// NOTE: gc's the rows_pointer object on EOF and returns nil. LastError is set to EOF
// may need to be **C.char?
//export NextRow
func NextRow(rows_pointer unsafe.Pointer) **C.char {
  decode := gopointer.Restore(rows_pointer)
  var rows *sql.Rows
  
  if decode != nil {
    rows = decode.(*sql.Rows)
  } else {
    last_error = errors.New("rows_pointer invalid: Restore returned nil")
    return nil
  }
  
  if rows.Next() {
    columns, _ := rows.Columns()
    rowLength := len(columns)
    
    rawResult := make([][]byte, rowLength)
    rawData := make([]interface{}, rowLength)
    for i, _ := range rawResult {  // found in stackoverflow, fwiw
        rawData[i] = &rawResult[i] // Put pointers to each string in the interface slice
    }

    // https://stackoverflow.com/questions/58866962/how-to-pass-an-array-of-strings-and-get-an-array-of-strings-in-ruby-using-go-sha
    pointerSize := unsafe.Sizeof(rows_pointer)
    // Allocate an array for the string pointers.
    var out **C.char
    out = (**C.char)(C.malloc(C.ulong(rowLength) * C.ulong(pointerSize)))

    last_error = rows.Scan(rawData...)
    if last_error != nil {
      return nil
    }
    pointer := out
    for _, raw := range rawResult {
      // Find where to store the address of the next string.
      // Copy each output string to a C string, and add it to the array.
      // C.CString uses malloc to allocate memory.
      if raw == nil {
        *pointer = nil
      } else {
        *pointer = C.CString(string(raw))
      }
      pointer = (**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(pointer)) + pointerSize))
    }
    return out
  } else if rows.Err() == io.EOF {
    gopointer.Unref(rows_pointer) // free up for gc
  }
  return nil
}

func main(){}
