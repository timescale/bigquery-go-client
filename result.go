package bigquery

import (
	"database/sql/driver"
	"errors"

	"cloud.google.com/go/bigquery"
)

var (
	_ driver.Result = (*result)(nil)
)

type result struct {
	iterator *bigquery.RowIterator
}

func (r *result) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId is not supported")
}

func (r *result) RowsAffected() (int64, error) {
	return int64(r.iterator.TotalRows), nil
}
