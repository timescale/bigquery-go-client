package bigquery

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("bigquery", &bigQueryDriver{})
}

var (
	_ driver.Driver        = (*bigQueryDriver)(nil)
	_ driver.DriverContext = (*bigQueryDriver)(nil)
)

type bigQueryDriver struct{}

func (b *bigQueryDriver) Open(dsn string) (driver.Conn, error) {
	connector, err := b.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (b *bigQueryDriver) OpenConnector(dsn string) (driver.Connector, error) {
	config, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return NewConnector(config), nil
}
