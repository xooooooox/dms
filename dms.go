package dms

import (
	"database/sql"
	"errors"
)

// database connect instance
var db *sql.DB

// SetDb set database connect instance
func SetDb(database *sql.DB) {
	db = database
}

// GetDb get database connect instance
func GetDb() *sql.DB {
	return db
}

// Query execute the most primitive query sql
func Query(get interface{}, query string, args ...interface{}) error {
	stmt, err := db.Prepare(query)
	if err != nil {
		return err
	}
	rows, err := stmt.Query(args...)
	if err != nil {
		return err
	}
	return Scan(rows, get)
}

// Exec execute the most primitive execute sql
func Exec(query string, args ...interface{}) (affectedRows int64, err error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return
	}
	result, err := stmt.Exec(args...)
	if err != nil {
		return
	}
	return result.RowsAffected()
}

// ExecGetInsertId execute insert sql, and get auto increment id value
func ExecGetInsertId(query string, args ...interface{}) (id int64, err error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return
	}
	result, err := stmt.Exec(args...)
	if err != nil {
		return
	}
	n, err := result.RowsAffected()
	if err != nil {
		return
	}
	if n == 0 {
		err = errors.New("the number of rows affected is 0")
		return
	}
	return result.LastInsertId()
}

// TxExec execute sql in transaction, and get affected rows value
func TxExec(tx *sql.Tx, query string, args ...interface{}) (affectedRows int64, err error) {
	stmt, err := tx.Prepare(query)
	if err != nil {
		return
	}
	result, err := stmt.Exec(args...)
	if err != nil {
		return
	}
	return result.RowsAffected()
}

// TxExecGetInsertId execute insert sql in transaction, and get auto increment id value
func TxExecGetInsertId(tx *sql.Tx, query string, args ...interface{}) (id int64, err error) {
	stmt, err := tx.Prepare(query)
	if err != nil {
		return
	}
	result, err := stmt.Exec(args...)
	if err != nil {
		return
	}
	n, err := result.RowsAffected()
	if err != nil {
		return
	}
	if n == 0 {
		err = errors.New("the number of rows affected is 0")
		return
	}
	return result.LastInsertId()
}
