package bigquery

import (
	"fmt"
	"reflect"

	"cloud.google.com/go/bigquery"
)

type invalidConnStrError struct {
	Err error
}

func (e *invalidConnStrError) Error() string {
	return fmt.Sprintf("invalid connection string: %v", e.Err)
}

func (e *invalidConnStrError) Unwrap() error {
	return e.Err
}

type invalidFieldTypeError struct {
	FieldType bigquery.FieldType
}

func (e *invalidFieldTypeError) Error() string {
	return fmt.Sprintf("invalid field type: %s", e.FieldType)
}

type unexpectedTypeError struct {
	FieldType bigquery.FieldType
	Expected  reflect.Type
	Actual    bigquery.Value
}

func (e *unexpectedTypeError) Error() string {
	return fmt.Sprintf(
		"received unexpected type: %T for BigQuery field: %s (expected: %s)",
		e.Actual, e.FieldType, e.Expected,
	)
}
