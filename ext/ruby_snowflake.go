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

// Lazy coding: storing last error and connection as global vars bc don't want to figure out how to pkg and pass them
// back and forth to ruby
var last_error error

//export LastError
func LastError() *C.char {
  if last_error == nil {
    return nil
  } else {
    return C.CString(last_error.Error())
  }
}

// @returns db pointer
// ugh, ruby and go were disagreeing about the length of `int` so I had to be particular here and in the ffi
//export Connect
func Connect(account *C.char, warehouse *C.char, database *C.char, schema *C.char,
  user *C.char, password *C.char, role *C.char, port int64) unsafe.Pointer {
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
    return nil
  }
  
  var db *sql.DB
  db, last_error = sql.Open("snowflake", dsn)
  if db == nil {
    return nil
  } else {
    return gopointer.Save(db)
  }
}

//export Close
func Close(db_pointer unsafe.Pointer) {
  db := decodeDbPointer(db_pointer)
  if db != nil {
    db.Close()
  }
}

// @return number of rows affected or -1 for error
//export Exec
func Exec(db_pointer unsafe.Pointer, statement *C.char) int64 {
  db := decodeDbPointer(db_pointer)
  var res sql.Result
  res, last_error = db.Exec(C.GoString(statement))
  if res != nil {
    rows, _ := res.RowsAffected()
    return rows
  }
  return -1
}

//export Fetch
func Fetch(db_pointer unsafe.Pointer, statement *C.char) unsafe.Pointer {
  db := decodeDbPointer(db_pointer)
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
//export NextRow
func NextRow(rows_pointer unsafe.Pointer) **C.char {
  if rows_pointer == nil {
    last_error = errors.New("rows_pointer null: cannot fetch")
    return nil    
  }
  var rows *sql.Rows
  rows = gopointer.Restore(rows_pointer).(*sql.Rows)
  
  if rows == nil {
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

func decodeDbPointer(db_pointer unsafe.Pointer) *sql.DB {
  if db_pointer == nil {
    last_error = errors.New("db_pointer is null. Cannot process command.")
    return nil
  }
  return gopointer.Restore(db_pointer).(*sql.DB)
}

func main(){}
