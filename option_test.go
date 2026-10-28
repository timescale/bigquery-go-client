package bigquery

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

func TestCheckNamedValue(t *testing.T) {
	tests := []struct {
		name         string
		value        any
		wantErr      error
		wantGetQuery bool
		wantGetJob   bool
	}{
		{
			name:         "GetQuery",
			value:        GetQuery(func(*bigquery.Query) {}),
			wantErr:      driver.ErrRemoveArgument,
			wantGetQuery: true,
		},
		{
			name:       "GetJob",
			value:      GetJob(func(*bigquery.Job) {}),
			wantErr:    driver.ErrRemoveArgument,
			wantGetJob: true,
		},
		{name: "civil.Date", value: civil.Date{Year: 2024, Month: 1, Day: 2}},
		{name: "civil.Time", value: civil.Time{Hour: 3}},
		{name: "civil.DateTime", value: civil.DateTime{}},
		{name: "Valuer", value: sql.NullString{}, wantErr: driver.ErrSkip},
		{name: "int64", value: int64(1)},
		{name: "string", value: "foo"},
		{name: "nil", value: nil},
		{name: "unsupported type", value: struct{}{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var o options
			err := o.CheckNamedValue(&driver.NamedValue{Value: tt.value})
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("CheckNamedValue() error = %v, want %v", err, tt.wantErr)
			}
			if got := o.getQuery != nil; got != tt.wantGetQuery {
				t.Errorf("getQuery set = %t, want %t", got, tt.wantGetQuery)
			}
			if got := o.getJob != nil; got != tt.wantGetJob {
				t.Errorf("getJob set = %t, want %t", got, tt.wantGetJob)
			}
		})
	}
}

func TestOptionsReset(t *testing.T) {
	o := options{
		getQuery: func(*bigquery.Query) {},
		getJob:   func(*bigquery.Job) {},
	}
	o.reset()
	if o.getQuery != nil || o.getJob != nil {
		t.Error("reset() did not clear options")
	}
}
