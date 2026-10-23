package bigquery

import (
	"context"
	"database/sql/driver"
)

var (
	_ driver.Tx = (*tx)(nil)
)

type tx struct {
	conn *conn
}

func (t *tx) Commit() error {
	return t.conn.exec(context.Background(), "COMMIT TRANSACTION;")
}

func (t *tx) Rollback() error {
	return t.conn.exec(context.Background(), "ROLLBACK TRANSACTION;")
}
