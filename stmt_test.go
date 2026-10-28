package bigquery

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

func TestBuildParameters(t *testing.T) {
	tests := []struct {
		name string
		args []driver.NamedValue
		want []bigquery.QueryParameter
	}{
		{
			name: "no args",
			args: nil,
			want: []bigquery.QueryParameter{},
		},
		{
			name: "positional",
			args: []driver.NamedValue{
				{Ordinal: 1, Value: int64(1)},
				{Ordinal: 2, Value: "foo"},
			},
			want: []bigquery.QueryParameter{
				{Value: int64(1)},
				{Value: "foo"},
			},
		},
		{
			name: "named",
			args: []driver.NamedValue{
				{Name: "a", Ordinal: 1, Value: true},
				{Name: "b", Ordinal: 2, Value: nil},
			},
			want: []bigquery.QueryParameter{
				{Name: "a", Value: true},
				{Name: "b", Value: nil},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &stmt{conn: &conn{}}
			got := s.buildParameters(tt.args)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildParameters() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestBuildConnectionProperties(t *testing.T) {
	tests := []struct {
		name      string
		sessionID string
		want      []*bigquery.ConnectionProperty
	}{
		{
			name:      "no session",
			sessionID: "",
			want:      nil,
		},
		{
			name:      "session",
			sessionID: "abc123",
			want: []*bigquery.ConnectionProperty{
				{Key: "session_id", Value: "abc123"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &stmt{conn: &conn{sessionID: tt.sessionID}}
			got := s.buildConnectionProperties()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildConnectionProperties() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestSessionError(t *testing.T) {
	const sessionID = "abc123"
	expiredMsg := fmt.Sprintf("Session %s has expired and is no longer available.", sessionID)
	expiredErr := &googleapi.Error{
		Code: http.StatusBadRequest,
		Errors: []googleapi.ErrorItem{
			{Reason: "resourcesExceeded", Message: expiredMsg},
		},
	}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "expired session",
			err:  expiredErr,
			want: true,
		},
		{
			name: "wrapped expired session",
			err:  fmt.Errorf("wrapped: %w", expiredErr),
			want: true,
		},
		{
			name: "expired session among other errors",
			err: &googleapi.Error{
				Code: http.StatusBadRequest,
				Errors: []googleapi.ErrorItem{
					{Reason: "invalid", Message: "something else"},
					{Reason: "resourcesExceeded", Message: expiredMsg},
				},
			},
			want: true,
		},
		{
			name: "different session",
			err: &googleapi.Error{
				Code: http.StatusBadRequest,
				Errors: []googleapi.ErrorItem{
					{Reason: "resourcesExceeded", Message: "Session other has expired and is no longer available."},
				},
			},
			want: false,
		},
		{
			name: "different reason",
			err: &googleapi.Error{
				Code: http.StatusBadRequest,
				Errors: []googleapi.ErrorItem{
					{Reason: "invalid", Message: expiredMsg},
				},
			},
			want: false,
		},
		{
			name: "different status code",
			err: &googleapi.Error{
				Code: http.StatusInternalServerError,
				Errors: []googleapi.ErrorItem{
					{Reason: "resourcesExceeded", Message: expiredMsg},
				},
			},
			want: false,
		},
		{
			name: "no error items",
			err:  &googleapi.Error{Code: http.StatusBadRequest},
			want: false,
		},
		{
			name: "not a googleapi error",
			err:  errors.New(expiredMsg),
			want: false,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sessionError(sessionID, tt.err); got != tt.want {
				t.Errorf("sessionError() = %t, want %t", got, tt.want)
			}
		})
	}
}
