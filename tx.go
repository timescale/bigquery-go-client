package driver

import "database/sql/driver"

var (
	_ driver.Tx = (*bigQueryTransaction)(nil)
)

// TODO: BEGIN and COMMIT
type bigQueryTransaction struct {
	connection *conn
}

func (t *bigQueryTransaction) Commit() error {
	return nil
}

func (t *bigQueryTransaction) Rollback() error {
	return nil
}
