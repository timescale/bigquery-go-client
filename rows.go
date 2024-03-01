package driver

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
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
		return convertBasicType[string](field.Type, value)
	case bigquery.BytesFieldType:
		return convertBasicType[[]byte](field.Type, value)
	case bigquery.IntegerFieldType:
		return convertBasicType[int64](field.Type, value)
	case bigquery.FloatFieldType:
		return convertBasicType[float64](field.Type, value)
	case bigquery.BooleanFieldType:
		return convertBasicType[bool](field.Type, value)
	case bigquery.TimestampFieldType:
		return convertBasicType[time.Time](field.Type, value)
	case bigquery.RecordFieldType:
		// TODO: Inflate RECORD types
		// []bigquery.Value
	case bigquery.DateFieldType:
		return convertStringType[civil.Date](field.Type, value)
	case bigquery.TimeFieldType:
		return convertStringType[civil.Time](field.Type, value)
	case bigquery.DateTimeFieldType:
		return convertStringType[civil.DateTime](field.Type, value)
	case bigquery.NumericFieldType:
		return convertRatType(field.Type, value, bigquery.NumericString)
	case bigquery.BigNumericFieldType:
		return convertRatType(field.Type, value, bigquery.BigNumericString)
	case bigquery.GeographyFieldType:
		return convertBasicType[string](field.Type, value)
	case bigquery.IntervalFieldType:
		return convertBasicType[string](field.Type, value)
	case bigquery.JSONFieldType:
		return convertBasicType[string](field.Type, value)
	case bigquery.RangeFieldType:
		return convertBasicType[string](field.Type, value)
	default:
		return nil, &InvalidFieldTypeError{
			FieldType: field.Type,
		}
	}

	return value, nil
}

func convertBasicType[T any](fieldType bigquery.FieldType, value bigquery.Value) (driver.Value, error) {
	switch val := value.(type) {
	case nil, T, *T:
		return val, nil
	default:
		return nil, &UnexpectedTypeError{
			FieldType: fieldType,
			Expected:  reflect.TypeFor[T](),
			Actual:    val,
		}
	}
}

func convertStringType[T fmt.Stringer](fieldType bigquery.FieldType, value bigquery.Value) (driver.Value, error) {
	switch val := value.(type) {
	case nil:
		return val, nil
	case T:
		return val.String(), nil
	default:
		return nil, &UnexpectedTypeError{
			FieldType: fieldType,
			Expected:  reflect.TypeFor[T](),
			Actual:    val,
		}
	}
}

type ratToStr func(*big.Rat) string

func convertRatType(fieldType bigquery.FieldType, value bigquery.Value, toStr ratToStr) (driver.Value, error) {
	switch val := value.(type) {
	case nil:
		return val, nil
	case *big.Rat:
		// Attempt to use the minimum number of digits after the decimal point,
		// if the resulting number will be exact.
		if prec, exact := val.FloatPrec(); exact {
			return val.FloatString(prec), nil
		}

		// Otherwise, fallback to default string conversion function, which
		// uses the maximum number of digits supported by BigQuery.
		return toStr(val), nil
	default:
		return nil, &UnexpectedTypeError{
			FieldType: fieldType,
			Expected:  reflect.TypeFor[*big.Rat](),
			Actual:    val,
		}
	}
}
