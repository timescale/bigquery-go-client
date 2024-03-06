package bigquery

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
)

var (
	_ driver.Conn               = (*conn)(nil)
	_ driver.Pinger             = (*conn)(nil)
	_ driver.Validator          = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
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
	// TODO: Return false if session has ended
	// (can connection be broken in any other way?)
	return true
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
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
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if level := sql.IsolationLevel(opts.Isolation); level != sql.LevelDefault {
		return nil, fmt.Errorf("invalid isolation level (only sql.LevelDefault is supported): %s", level)
	}

	if opts.ReadOnly {
		return nil, errors.New("read-only transactions not supported")
	}

	if _, err := c.ExecContext(ctx, "BEGIN TRANSACTION", nil); err != nil {
		return nil, err
	}

	return &tx{conn: c}, nil
}
