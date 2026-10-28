package bigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestNewResult(t *testing.T) {
	tests := []struct {
		name   string
		status *bigquery.JobStatus
		want   int64
	}{
		{
			name:   "no statistics",
			status: &bigquery.JobStatus{},
			want:   0,
		},
		{
			name: "non-query statistics",
			status: &bigquery.JobStatus{
				Statistics: &bigquery.JobStatistics{
					Details: &bigquery.LoadStatistics{},
				},
			},
			want: 0,
		},
		{
			name: "query statistics",
			status: &bigquery.JobStatus{
				Statistics: &bigquery.JobStatistics{
					Details: &bigquery.QueryStatistics{NumDMLAffectedRows: 42},
				},
			},
			want: 42,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := newResult(tt.status)
			got, err := res.RowsAffected()
			if err != nil {
				t.Fatalf("RowsAffected() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("RowsAffected() = %d, want %d", got, tt.want)
			}
			if _, err := res.LastInsertId(); err == nil {
				t.Error("LastInsertId() error = nil, want error")
			}
		})
	}
}
