package bigquery

import (
	"errors"
	"math/big"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

func TestColumnType(t *testing.T) {
	tests := []struct {
		name  string
		field *bigquery.FieldSchema
		want  string
	}{
		{
			name:  "BOOL",
			field: &bigquery.FieldSchema{Type: bigquery.BooleanFieldType},
			want:  "BOOL",
		},
		{
			name:  "INT64",
			field: &bigquery.FieldSchema{Type: bigquery.IntegerFieldType},
			want:  "INT64",
		},
		{
			name:  "FLOAT64",
			field: &bigquery.FieldSchema{Type: bigquery.FloatFieldType},
			want:  "FLOAT64",
		},
		{
			name:  "STRING",
			field: &bigquery.FieldSchema{Type: bigquery.StringFieldType},
			want:  "STRING",
		},
		{
			name:  "TIMESTAMP",
			field: &bigquery.FieldSchema{Type: bigquery.TimestampFieldType},
			want:  "TIMESTAMP",
		},
		{
			name: "RANGE",
			field: &bigquery.FieldSchema{
				Type:             bigquery.RangeFieldType,
				RangeElementType: &bigquery.RangeElementType{Type: bigquery.DateFieldType},
			},
			want: "RANGE<DATE>",
		},
		{
			name:  "ARRAY",
			field: &bigquery.FieldSchema{Type: bigquery.IntegerFieldType, Repeated: true},
			want:  "ARRAY<INT64>",
		},
		{
			name: "STRUCT",
			field: &bigquery.FieldSchema{
				Type: bigquery.RecordFieldType,
				Schema: bigquery.Schema{
					{Name: "a", Type: bigquery.IntegerFieldType},
					{Name: "b", Type: bigquery.StringFieldType, Repeated: true},
				},
			},
			want: "STRUCT<INT64,ARRAY<STRING>>",
		},
		{
			name: "ARRAY of nested STRUCT",
			field: &bigquery.FieldSchema{
				Type:     bigquery.RecordFieldType,
				Repeated: true,
				Schema: bigquery.Schema{
					{Name: "a", Type: bigquery.BooleanFieldType},
					{
						Name: "b",
						Type: bigquery.RecordFieldType,
						Schema: bigquery.Schema{
							{Name: "c", Type: bigquery.FloatFieldType},
						},
					},
				},
			},
			want: "ARRAY<STRUCT<BOOL,STRUCT<FLOAT64>>>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := columnType(tt.field); got != tt.want {
				t.Errorf("columnType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConvertValue(t *testing.T) {
	ts := time.Date(2024, 1, 2, 3, 4, 5, 6, time.UTC)

	field := func(typ bigquery.FieldType) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Type: typ}
	}
	repeated := func(typ bigquery.FieldType) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{Type: typ, Repeated: true}
	}
	rangeField := func(typ bigquery.FieldType) *bigquery.FieldSchema {
		return &bigquery.FieldSchema{
			Type:             bigquery.RangeFieldType,
			RangeElementType: &bigquery.RangeElementType{Type: typ},
		}
	}
	record := &bigquery.FieldSchema{
		Type: bigquery.RecordFieldType,
		Schema: bigquery.Schema{
			{Name: "i", Type: bigquery.IntegerFieldType},
			{Name: "s", Type: bigquery.StringFieldType, Repeated: true},
			{Name: "j", Type: bigquery.JSONFieldType},
			{Name: "b", Type: bigquery.BytesFieldType},
		},
	}

	tests := []struct {
		name  string
		field *bigquery.FieldSchema
		value bigquery.Value
		want  any
	}{
		// Basic types.
		{"STRING", field(bigquery.StringFieldType), "foo", "foo"},
		{"BYTES", field(bigquery.BytesFieldType), []byte("foo"), []byte("foo")},
		{"INT64", field(bigquery.IntegerFieldType), int64(42), int64(42)},
		{"FLOAT64", field(bigquery.FloatFieldType), 1.5, 1.5},
		{"BOOL", field(bigquery.BooleanFieldType), true, true},
		{"TIMESTAMP", field(bigquery.TimestampFieldType), ts, ts},
		{"GEOGRAPHY", field(bigquery.GeographyFieldType), "POINT(1 2)", "POINT(1 2)"},

		// Stringer types.
		{
			"DATE", field(bigquery.DateFieldType),
			civil.Date{Year: 2024, Month: 1, Day: 2},
			"2024-01-02",
		},
		{
			"TIME", field(bigquery.TimeFieldType),
			civil.Time{Hour: 3, Minute: 4, Second: 5, Nanosecond: 6000},
			"03:04:05.000006000",
		},
		{
			"DATETIME", field(bigquery.DateTimeFieldType),
			civil.DateTime{
				Date: civil.Date{Year: 2024, Month: 1, Day: 2},
				Time: civil.Time{Hour: 3, Minute: 4, Second: 5},
			},
			"2024-01-02T03:04:05",
		},
		{
			"INTERVAL", field(bigquery.IntervalFieldType),
			&bigquery.IntervalValue{Years: 1, Months: 2, Days: 3, Hours: 4, Minutes: 5, Seconds: 6},
			"1-2 3 4:5:6",
		},

		// Rational types.
		{"NUMERIC exact", field(bigquery.NumericFieldType), big.NewRat(5, 4), "1.25"},
		{"NUMERIC integer", field(bigquery.NumericFieldType), big.NewRat(10, 1), "10"},
		{"NUMERIC inexact", field(bigquery.NumericFieldType), big.NewRat(1, 3), "0.333333333"},
		{"BIGNUMERIC exact", field(bigquery.BigNumericFieldType), big.NewRat(-1, 8), "-0.125"},
		{
			"BIGNUMERIC inexact", field(bigquery.BigNumericFieldType),
			big.NewRat(1, 3),
			"0.33333333333333333333333333333333333333",
		},

		// JSON.
		{"JSON", field(bigquery.JSONFieldType), `{"a":1}`, []byte(`{"a":1}`)},

		// RANGE.
		{
			"RANGE<DATE>", rangeField(bigquery.DateFieldType),
			&bigquery.RangeValue{
				Start: civil.Date{Year: 2024, Month: 1, Day: 1},
				End:   civil.Date{Year: 2024, Month: 2, Day: 1},
			},
			"[2024-01-01, 2024-02-01)",
		},
		{
			"RANGE<TIMESTAMP>", rangeField(bigquery.TimestampFieldType),
			&bigquery.RangeValue{Start: ts, End: ts.Add(time.Hour)},
			"[2024-01-02T03:04:05.000000006Z, 2024-01-02T04:04:05.000000006Z)",
		},
		{
			"RANGE unbounded", rangeField(bigquery.DateFieldType),
			&bigquery.RangeValue{},
			"[UNBOUNDED, UNBOUNDED)",
		},
		{
			"RANGE nil pointer", rangeField(bigquery.DateFieldType),
			(*bigquery.RangeValue)(nil),
			nil,
		},

		// ARRAY.
		{
			"ARRAY<INT64>", repeated(bigquery.IntegerFieldType),
			[]bigquery.Value{int64(1), int64(2)},
			[]byte(`[1,2]`),
		},
		{
			"ARRAY<STRING> with nil", repeated(bigquery.StringFieldType),
			[]bigquery.Value{"a", nil},
			[]byte(`["a",null]`),
		},
		{
			"ARRAY<JSON>", repeated(bigquery.JSONFieldType),
			[]bigquery.Value{`{"a":1}`, `[2]`},
			[]byte(`[{"a":1},[2]]`),
		},
		{
			"ARRAY<BYTES>", repeated(bigquery.BytesFieldType),
			[]bigquery.Value{[]byte("foo")},
			[]byte(`["Zm9v"]`),
		},
		{
			"ARRAY empty", repeated(bigquery.IntegerFieldType),
			[]bigquery.Value{},
			[]byte(`[]`),
		},

		// STRUCT.
		{
			"STRUCT", record,
			[]bigquery.Value{
				int64(1),
				[]bigquery.Value{"x", "y"},
				`{"k":"v"}`,
				[]byte("foo"),
			},
			[]byte(`{"b":"Zm9v","i":1,"j":{"k":"v"},"s":["x","y"]}`),
		},
		{
			"STRUCT with nils", record,
			[]bigquery.Value{nil, nil, nil, nil},
			[]byte(`{"b":null,"i":null,"j":null,"s":null}`),
		},
		{
			"ARRAY<STRUCT>",
			&bigquery.FieldSchema{
				Type:     bigquery.RecordFieldType,
				Repeated: true,
				Schema:   bigquery.Schema{{Name: "a", Type: bigquery.IntegerFieldType}},
			},
			[]bigquery.Value{
				[]bigquery.Value{int64(1)},
				[]bigquery.Value{int64(2)},
			},
			[]byte(`[{"a":1},{"a":2}]`),
		},
	}

	// NULL is passed through for every type.
	for _, f := range []*bigquery.FieldSchema{
		field(bigquery.StringFieldType),
		field(bigquery.BytesFieldType),
		field(bigquery.IntegerFieldType),
		field(bigquery.FloatFieldType),
		field(bigquery.BooleanFieldType),
		field(bigquery.TimestampFieldType),
		field(bigquery.DateFieldType),
		field(bigquery.TimeFieldType),
		field(bigquery.DateTimeFieldType),
		field(bigquery.NumericFieldType),
		field(bigquery.BigNumericFieldType),
		field(bigquery.GeographyFieldType),
		field(bigquery.IntervalFieldType),
		field(bigquery.JSONFieldType),
		rangeField(bigquery.DateFieldType),
	} {
		tests = append(tests, struct {
			name  string
			field *bigquery.FieldSchema
			value bigquery.Value
			want  any
		}{columnType(f) + " NULL", f, nil, nil})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValue(tt.field, tt.value)
			if err != nil {
				t.Fatalf("convertValue() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertValue() = %#v (%s), want %#v", got, got, tt.want)
			}
		})
	}
}

func TestConvertValueErrors(t *testing.T) {
	tests := []struct {
		name  string
		field *bigquery.FieldSchema
		value bigquery.Value
		want  any
	}{
		{
			name:  "invalid field type",
			field: &bigquery.FieldSchema{Type: "BOGUS"},
			value: "foo",
			want:  &invalidFieldTypeError{},
		},
		{
			name:  "basic type mismatch",
			field: &bigquery.FieldSchema{Type: bigquery.IntegerFieldType},
			value: "foo",
			want:  &unexpectedTypeError{},
		},
		{
			name:  "stringer type mismatch",
			field: &bigquery.FieldSchema{Type: bigquery.DateFieldType},
			value: "2024-01-02",
			want:  &unexpectedTypeError{},
		},
		{
			name:  "rational type mismatch",
			field: &bigquery.FieldSchema{Type: bigquery.NumericFieldType},
			value: 1.5,
			want:  &unexpectedTypeError{},
		},
		{
			name:  "JSON type mismatch",
			field: &bigquery.FieldSchema{Type: bigquery.JSONFieldType},
			value: []byte(`{}`),
			want:  &unexpectedTypeError{},
		},
		{
			name: "range type mismatch",
			field: &bigquery.FieldSchema{
				Type:             bigquery.RangeFieldType,
				RangeElementType: &bigquery.RangeElementType{Type: bigquery.DateFieldType},
			},
			value: "[2024-01-01, 2024-02-01)",
			want:  &unexpectedTypeError{},
		},
		{
			name: "range bound type mismatch",
			field: &bigquery.FieldSchema{
				Type:             bigquery.RangeFieldType,
				RangeElementType: &bigquery.RangeElementType{Type: bigquery.DateFieldType},
			},
			value: &bigquery.RangeValue{Start: int64(1)},
			want:  &unexpectedTypeError{},
		},
		{
			name:  "repeated not a slice",
			field: &bigquery.FieldSchema{Type: bigquery.IntegerFieldType, Repeated: true},
			value: int64(1),
			want:  &unexpectedTypeError{},
		},
		{
			name:  "repeated element mismatch",
			field: &bigquery.FieldSchema{Type: bigquery.IntegerFieldType, Repeated: true},
			value: []bigquery.Value{int64(1), "foo"},
			want:  &unexpectedTypeError{},
		},
		{
			name: "record not a slice",
			field: &bigquery.FieldSchema{
				Type:   bigquery.RecordFieldType,
				Schema: bigquery.Schema{{Name: "a", Type: bigquery.IntegerFieldType}},
			},
			value: map[string]bigquery.Value{"a": int64(1)},
			want:  &unexpectedTypeError{},
		},
		{
			name: "record field mismatch",
			field: &bigquery.FieldSchema{
				Type:   bigquery.RecordFieldType,
				Schema: bigquery.Schema{{Name: "a", Type: bigquery.IntegerFieldType}},
			},
			value: []bigquery.Value{"foo"},
			want:  &unexpectedTypeError{},
		},
		{
			name:  "invalid JSON",
			field: &bigquery.FieldSchema{Type: bigquery.JSONFieldType, Repeated: true},
			value: []bigquery.Value{`{not json`},
			want:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := convertValue(tt.field, tt.value)
			if err == nil {
				t.Fatal("convertValue() error = nil, want error")
			}
			if tt.want == nil {
				return
			}
			target := reflect.New(reflect.TypeOf(tt.want))
			if !errors.As(err, target.Interface()) {
				t.Errorf("convertValue() error = %v (%T), want %T", err, err, tt.want)
			}
		})
	}
}
