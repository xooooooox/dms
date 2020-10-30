package dms

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

// ScanStruct get: *struct
func ScanStruct(rows *sql.Rows, get interface{}) error {
	if rows == nil {
		return errors.New("sql query rows is nil")
	}
	t := reflect.TypeOf(get)
	k := t.Kind()
	if reflect.Ptr != k {
		return errors.New("the parameter to receive data is not a pointer, require *struct")
	}
	k = t.Elem().Kind()
	if reflect.Struct != k {
		return errors.New("the parameter to receive data is not a structure pointer, require *struct")
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	// column value
	var field reflect.Value
	// reflect zero value
	reflectZeroValue := reflect.Value{}
	// new struct type (pointer)
	result := reflect.New(t.Elem())
	// get new struct value
	resultValue := reflect.Indirect(result)
	// rows scan columns list
	scans := []interface{}{}
	// if rows.Scan() has been executed once, do not execute it again, but rows.Next() must be continue run again and again, because maybe select more rows in sql
	hadScan := false
	for rows.Next() {
		if hadScan {
			continue
		}
		for _, col := range cols {
			field = resultValue.FieldByName(UnderlineToPascal(col))
			if reflectZeroValue == field {
				return errors.New(fmt.Sprintf("can not find the corresponding field <%s> found in the structure <%s>", col, t.Name()))
			}
			if !field.CanSet() {
				return errors.New(fmt.Sprintf("unable to set value, corresponding field <%s> was found in structure <%s>", col, t.Name()))
			}
			scans = append(scans, field.Addr().Interface())
		}
		err = rows.Scan(scans...)
		if err != nil {
			return err
		}
		hadScan = true
	}
	reflect.ValueOf(get).Elem().Set(result.Elem())
	return nil
}

// ScanSlice scan more rows from sql query, get must be a pointer, get: *[]struct or *[]*struct
func ScanSlice(rows *sql.Rows, get interface{}) error {
	if rows == nil {
		return errors.New("sql query rows is nil")
	}
	t := reflect.TypeOf(get)
	k := t.Kind()
	if reflect.Ptr != k {
		return errors.New("the parameter to receive data is not a pointer, require *[]struct or *[]*struct")
	}
	k = t.Elem().Kind()
	if reflect.Slice != k {
		return errors.New("the parameter to receive data is not a pointer, require *[]struct or *[]*struct")
	}
	k = t.Elem().Elem().Kind()
	// reflect zero value
	reflectZeroValue := reflect.Value{}
	switch k {
	// *[]struct
	case reflect.Struct:
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		// []struct
		result := reflect.ValueOf(get).Elem()
		// reflect zero value
		reflectZeroValue := reflect.Value{}
		// new struct type (pointer)
		rowResult := reflect.New(t.Elem().Elem())
		// get new struct value
		rowResultValue := reflect.Indirect(rowResult)
		// rows scan columns list
		scans := []interface{}{}
		for _, col := range cols {
			filed := rowResultValue.FieldByName(UnderlineToPascal(col))
			if reflectZeroValue == filed {
				return errors.New(fmt.Sprintf("can not find the corresponding field <%s> found in the structure <%s>", col, t.Elem().Elem().Name()))
			}
			if !filed.CanSet() {
				return errors.New(fmt.Sprintf("unable to set value, corresponding field <%s> was found in structure <%s>", col, t.Elem().Elem().Name()))
			}
			scans = append(scans, filed.Addr().Interface())
		}
		for rows.Next() {
			err := rows.Scan(scans...)
			if err != nil {
				return err
			}
			result = reflect.Append(result, rowResult.Elem())
		}
		reflect.ValueOf(get).Elem().Set(result)
	// *[]*struct
	case reflect.Ptr:
		if reflect.Struct != t.Elem().Elem().Elem().Kind() {
			return errors.New("the parameter to receive data is not a pointer, require *[]struct or *[]*struct")
		}
		cols, err := rows.Columns()
		if err != nil {
			return err
		}
		// []*struct
		result := reflect.ValueOf(get).Elem()
		var rowResult reflect.Value
		var rowResultValue reflect.Value
		var scans []interface{}
		for rows.Next() {
			// new struct type (pointer)
			rowResult = reflect.New(t.Elem().Elem().Elem())
			// get new struct value
			rowResultValue = reflect.Indirect(rowResult)
			// rows scan columns list
			scans = []interface{}{}
			for _, col := range cols {
				filed := rowResultValue.FieldByName(UnderlineToPascal(col))
				if reflectZeroValue == filed {
					return errors.New(fmt.Sprintf("can not find the corresponding field <%s> found in the structure <%s>", col, t.Elem().Elem().Elem().Name()))
				}
				if !filed.CanSet() {
					return errors.New(fmt.Sprintf("unable to set value, corresponding field <%s> was found in structure <%s>", col, t.Elem().Elem().Elem().Name()))
				}
				scans = append(scans, filed.Addr().Interface())
			}
			err = rows.Scan(scans...)
			if err != nil {
				return err
			}
			result = reflect.Append(result, rowResult)
		}
		reflect.ValueOf(get).Elem().Set(result)
	default:
		return errors.New("the parameter to receive data is not a pointer, require *[]struct or *[]*struct")
	}
	return nil
}

// Scan scan one or more rows from query to pointer parameter
func Scan(rows *sql.Rows, get interface{}) error {
	t := reflect.TypeOf(get)
	k := t.Kind()
	if reflect.Ptr != k {
		return errors.New("please input a pointer")
	}
	k = t.Elem().Kind()
	switch k {
	case reflect.Struct:
		return ScanStruct(rows, get)
	case reflect.Slice:
		return ScanSlice(rows, get)
	default:
		return errors.New(fmt.Sprintf("checking out data to type <%s> is not supported", t.Name()))
	}
}
