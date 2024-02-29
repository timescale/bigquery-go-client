package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"

	"golang.org/x/oauth2/google"
)

func init() {
	sql.Register("bigquery", &bigQueryDriver{})
}

var (
	_ driver.Driver        = (*bigQueryDriver)(nil)
	_ driver.DriverContext = (*bigQueryDriver)(nil)
)

type bigQueryDriver struct{}

type Config struct {
	ProjectID       string
	Dataset         string
	Scopes          []string
	Endpoint        string
	DisableAuth     bool
	Credentials     *google.Credentials
	CredentialsFile string
	CredentialsJSON []byte
}

func (b *bigQueryDriver) Open(dsn string) (driver.Conn, error) {
	config, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}

	connector := NewConnector(config)
	return connector.Connect(context.Background())
}

func (b *bigQueryDriver) OpenConnector(dsn string) (driver.Connector, error) {
	config, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return NewConnector(config), nil
}

func parseDSN(dsn string) (Config, error) {
	url, err := url.Parse(dsn)
	if err != nil {
		return Config{}, fmt.Errorf("invalid connection string: %w", err)
	}
	if url.Scheme != "bigquery" {
		return Config{}, fmt.Errorf("invalid prefix, expected bigquery:// got: %s", dsn)
	}
	query := url.Query()

	return Config{
		ProjectID:       url.Hostname(),
		Dataset:         url.Path,
		Scopes:          query["scopes"],
		Endpoint:        query.Get("endpoint"),
		DisableAuth:     query.Get("disable_auth") == "true",
		CredentialsFile: query.Get("credential_file"),
	}, nil
}
