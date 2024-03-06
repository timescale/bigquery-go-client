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
	_, err := t.conn.ExecContext(context.Background(), "COMMIT TRANSACTION", nil)
	return err
}

func (t *tx) Rollback() error {
	_, err := t.conn.ExecContext(context.Background(), "ROLLBACK TRANSACTION", nil)
	return err
}
