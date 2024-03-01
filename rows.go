package driver

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

var (
	_ driver.Rows                           = (*rows)(nil)
	_ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
	_ driver.RowsColumnTypeLength           = (*rows)(nil)
	_ driver.RowsColumnTypePrecisionScale   = (*rows)(nil)

	// TODO:
	// _ driver.RowsColumnTypeScanType         = (*rows)(nil)
)

type rows struct {
	iterator   *bigquery.RowIterator
	nextCalled bool
	prevValues []bigquery.Value
	prevErr    error
}

func (r *rows) Columns() []string {
	schema := r.schema()
	columns := make([]string, len(schema))
	for i, field := range schema {
		columns[i] = field.Name
	}
	return columns
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	field := r.schema()[index]
	return string(field.Type)
}

func (r *rows) ColumnTypeLength(index int) (int64, bool) {
	field := r.schema()[index]
	ok := field.MaxLength != 0
	return field.MaxLength, ok
}

func (r *rows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	field := r.schema()[index]
	ok := field.Precision != 0 || field.Scale != 0
	return field.Precision, field.Scale, ok
}

func (r *rows) Close() error {
	var vals []bigquery.Value
	for r.iterator.Next(&vals) == nil {
		// Drain iterator
	}
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	values, err := r.prevOrNext()
	if err != nil {
		return err
	}

	schema := r.schema()
	for idx := range dest {
		value, err := r.convertValue(schema[idx], values[idx])
		if err != nil {
			return err
		}
		dest[idx] = value
	}
	return nil
}

func (r *rows) schema() bigquery.Schema {
	// Must call next before we can access the schema.
	// Cache the result/error for later.
	if !r.nextCalled {
		r.prevValues, r.prevErr = r.next()
	}
	return r.iterator.Schema
}

func (r *rows) prevOrNext() ([]bigquery.Value, error) {
	if r.prevValues != nil || r.prevErr != nil {
		values, err := r.prevValues, r.prevErr
		r.prevValues, r.prevErr = nil, nil
		return values, err
	}
	return r.next()
}

func (r *rows) next() ([]bigquery.Value, error) {
	r.nextCalled = true

	var values []bigquery.Value
	if err := r.iterator.Next(&values); err != nil {
		if err == iterator.Done {
			return nil, io.EOF
		}
		return nil, err
	}
	return values, nil
}

var (
	stringType  = reflect.TypeFor[string]()
	bytesType   = reflect.TypeFor[[]byte]()
	int64Type   = reflect.TypeFor[int64]()
	float64Type = reflect.TypeFor[float64]()
	timeType    = reflect.TypeFor[time.Time]()
)

func (r *rows) convertValue(field *bigquery.FieldSchema, value bigquery.Value) (driver.Value, error) {
	if field.Repeated {
		// TODO: Inflate RECORD types before marshalling
		out, err := json.Marshal(field)
		if err != nil {
			return nil, fmt.Errorf("error marshalling repeated field to JSON: %w", err)
		}
		return out, nil
	}

	switch field.Type {
	case bigquery.StringFieldType:
		// string
	case bigquery.BytesFieldType:
		// []byte
	case bigquery.IntegerFieldType:
		// int64
	case bigquery.FloatFieldType:
		// float64
	case bigquery.BooleanFieldType:
		// bool
	case bigquery.TimestampFieldType:
		// time.Time
	case bigquery.RecordFieldType:
		// TODO: Inflate RECORD types
		// []bigquery.Value
	case bigquery.DateFieldType:
		// TODO: Convert to string (or time?)
		// civil.Date
	case bigquery.TimeFieldType:
		// TODO: Convert to string (or time?)
		// civil.Time
	case bigquery.DateTimeFieldType:
		// TODO: Convert to string (or time?)
		// civil.DateTime
	case bigquery.NumericFieldType:
		// TODO: Convert to string (?)
		// *big.Rat
	case bigquery.GeographyFieldType:
		// ???
	case bigquery.BigNumericFieldType:
		// TODO: Convert to string (?)
		// *big.Rat
	case bigquery.IntervalFieldType:
		// ???
	case bigquery.JSONFieldType:
		// ???
	case bigquery.RangeFieldType:
		// ???
	}

	return value, nil
}
