package bigquery

import (
	"fmt"
	"reflect"

	"cloud.google.com/go/bigquery"
)

type InvalidConnectionStringError struct {
	Err error
}

func (e *InvalidConnectionStringError) Error() string {
	return fmt.Sprintf("invalid connection string: %w", e.Err)
}

type UnexpectedTypeError struct {
	FieldType bigquery.FieldType
	Expected  reflect.Type
	Actual    bigquery.Value
}

func (e *UnexpectedTypeError) Error() string {
	return fmt.Sprintf(
		"received unexpected type: %T for BigQuery field: %s (expected: %s)",
		e.Actual, e.FieldType, e.Expected,
	)
}

type InvalidFieldTypeError struct {
	FieldType bigquery.FieldType
}

func (e *InvalidFieldTypeError) Error() string {
	return fmt.Sprintf("invalid field type: %s", e.FieldType)
}
