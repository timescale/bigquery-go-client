package driver

import (
	"context"
	"database/sql/driver"

	"cloud.google.com/go/bigquery"
)

var (
	_ driver.Conn           = (*conn)(nil)
	_ driver.Pinger         = (*conn)(nil)
	_ driver.Validator      = (*conn)(nil)
	_ driver.ExecerContext  = (*conn)(nil)
	_ driver.QueryerContext = (*conn)(nil)
)

type conn struct {
	client    *bigquery.Client
	config    Config
	sessionID string
	closed    bool
}

func (c *conn) Ping(ctx context.Context) error {
	if _, err := c.client.Dataset(c.config.Dataset).Metadata(ctx); err != nil {
		return err
	}
	return nil
}

func (c *conn) IsValid() bool {
	return true
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{
		connection: c,
		query:      query,
	}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	statement := &stmt{
		connection: c,
		query:      query,
	}
	return statement.QueryContext(ctx, args)
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	statement := &stmt{
		connection: c,
		query:      query,
	}
	return statement.ExecContext(ctx, args)
}

func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.client.Close()
}

func (c *conn) Begin() (driver.Tx, error) {
	return &bigQueryTransaction{c}, nil
}
