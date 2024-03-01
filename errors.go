package driver

import (
	"fmt"
	"reflect"

	"cloud.google.com/go/bigquery"
)

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
