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
	_ driver.SessionResetter    = (*conn)(nil)
	_ driver.ConnBeginTx        = (*conn)(nil)
	_ driver.ConnPrepareContext = (*conn)(nil)
	_ driver.ExecerContext      = (*conn)(nil)
	_ driver.QueryerContext     = (*conn)(nil)
	_ driver.NamedValueChecker  = (*conn)(nil)
)

type conn struct {
	client    *bigquery.Client
	config    Config
	sessionID string
	closed    bool
	invalid   bool
	options

	// broken is set when BEGIN/COMMIT/ROLLBACK fails, since the session may be
	// left in an open transaction. Unlike an invalid conn, its session still
	// exists, so only user statements are refused (the session is still aborted
	// when the conn is closed).
	broken bool
}

func (c *conn) Ping(ctx context.Context) error {
	if _, err := c.client.Dataset(c.config.Dataset).Metadata(ctx); err != nil {
		return err
	}
	return nil
}

func (c *conn) IsValid() bool {
	return !c.invalid && !c.broken && !c.closed
}

func (c *conn) ResetSession(ctx context.Context) error {
	if !c.IsValid() {
		return driver.ErrBadConn
	}
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{
		conn:  c,
		query: query,
	}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	statement := &stmt{
		conn:  c,
		query: query,
	}
	return statement.QueryContext(ctx, args)
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	statement := &stmt{
		conn:  c,
		query: query,
	}
	return statement.ExecContext(ctx, args)
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

	if c.broken {
		return nil, driver.ErrBadConn
	}

	if err := c.exec(ctx, "BEGIN TRANSACTION;"); err != nil {
		c.broken = true
		return nil, err
	}

	return &tx{conn: c}, nil
}

func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true

	var errs []error
	if err := c.abortSession(context.Background()); err != nil {
		errs = append(errs, err)
	}

	if err := c.client.Close(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (c *conn) abortSession(ctx context.Context) error {
	// An invalid conn's session has already expired, so there's nothing to abort.
	if c.sessionID == "" || c.invalid {
		return nil
	}

	if err := c.exec(ctx, "CALL BQ.ABORT_SESSION();"); err != nil {
		return err
	}

	c.sessionID = ""
	return nil
}

// exec runs a statement issued internally by the driver. Unlike ExecContext, it
// doesn't make an extra API call to fetch the job's statistics. It uses read
// because job.Read is the cheapest way to wait for a query to finish (Wait does
// the same, plus a status call), and no rows are fetched unless the returned
// iterator is advanced.
func (c *conn) exec(ctx context.Context, query string) error {
	statement := &stmt{
		conn:  c,
		query: query,
	}
	_, err := statement.read(ctx, nil)
	return err
}
