package driver

import (
	"context"
	"database/sql/driver"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
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
	var opts []option.ClientOption
	if c.config.Scopes != nil {
		opts = append(opts, option.WithScopes(c.config.Scopes...))
	}
	if c.config.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(c.config.Endpoint))
	}
	if c.config.DisableAuth {
		opts = append(opts, option.WithoutAuthentication())
	}
	if c.config.Credentials != nil {
		opts = append(opts, option.WithCredentials(c.config.Credentials))
	}
	if c.config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(c.config.CredentialsFile))
	}
	if c.config.CredentialsJSON != nil {
		opts = append(opts, option.WithCredentialsJSON(c.config.CredentialsJSON))
	}

	// NOTE: We can't pass the provided context to NewClient, or it will cease
	// working when the context is cancelled (whereas the context provided to
	// this function should only control the lifetime of the connection event
	// itself).
	client, err := bigquery.NewClient(context.Background(), c.config.ProjectID, opts...)
	if err != nil {
		return nil, err
	}

	return &conn{
		client: client,
		config: c.config,
	}, nil
}

func (c *connector) Driver() driver.Driver {
	return &bigQueryDriver{}
}
