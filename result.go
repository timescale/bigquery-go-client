package bigquery

import (
	"database/sql/driver"
	"errors"

	"cloud.google.com/go/bigquery"
)

var _ driver.Result = (*result)(nil)

type result struct {
	rowsAffected int64
}

func newResult(status *bigquery.JobStatus) *result {
	if status.Statistics == nil {
		return &result{}
	}
	stats, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok {
		return &result{}
	}
	return &result{
		rowsAffected: stats.NumDMLAffectedRows,
	}
}

func (r *result) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId is not supported")
}

func (r *result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
