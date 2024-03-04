package driver

import (
	"context"
	"database/sql/driver"

	"cloud.google.com/go/bigquery"
)

var (
	_ driver.Connector = (*connector)(nil)
)

type connector struct {
	config Config
}

func NewConnector(config Config) driver.Connector {
	return &connector{
		config: config,
	}
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	// NOTE: We can't pass the provided context to NewClient, or it will cease
	// working when the context is cancelled (whereas the context provided to
	// this function should only control the lifetime of the connection event
	// itself).
	client, err := bigquery.NewClient(
		context.Background(),
		c.config.ProjectID,
		c.config.Options...,
	)
	if err != nil {
		return nil, err
	}
	client.Location = c.config.Location

	return &conn{
		client: client,
		config: c.config,
	}, nil
}

func (c *connector) Driver() driver.Driver {
	return &bigQueryDriver{}
}
