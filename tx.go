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
	if err := t.conn.exec(context.Background(), "COMMIT TRANSACTION;"); err != nil {
		t.conn.broken = true
		return err
	}
	return nil
}

func (t *tx) Rollback() error {
	if err := t.conn.exec(context.Background(), "ROLLBACK TRANSACTION;"); err != nil {
		t.conn.broken = true
		return err
	}
	return nil
}
