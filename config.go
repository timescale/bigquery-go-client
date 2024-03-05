package driver

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	"google.golang.org/api/option"
)

type Config struct {
	ProjectID string
	Dataset   string
	Location  string
	Options   []option.ClientOption
}

// Parses DSN of the form:
// bigquery://projectID[/location][/dataset]?key=val
func parseDSN(dsn string) (Config, error) {
	url, err := url.Parse(dsn)
	if err != nil {
		return Config{}, &InvalidConnectionStringError{
			Err: err,
		}
	}

	if url.Scheme != "bigquery" {
		return Config{}, &InvalidConnectionStringError{
			Err: fmt.Errorf("invalid scheme: expected 'bigquery://', received: '%s'", url.Scheme),
		}
	}

	location, dataset, err := parseLocationDataset(url)
	if err != nil {
		return Config{}, &InvalidConnectionStringError{
			Err: err,
		}
	}

	options, err := parseOptions(url)
	if err != nil {
		return Config{}, &InvalidConnectionStringError{
			Err: err,
		}
	}

	return Config{
		ProjectID: url.Hostname(),
		Location:  location,
		Dataset:   dataset,
		Options:   options,
	}, nil
}

func parseLocationDataset(url *url.URL) (string, string, error) {
	fields := strings.Split(strings.Trim(url.Path, "/"), "/")
	switch len(fields) {
	case 0:
		return "", "", nil
	case 1:
		return "", fields[0], nil
	case 2:
		return fields[0], fields[1], nil
	default:
		return "", "", fmt.Errorf("too many path segments: %s", url.Path)
	}
}

func parseOptions(url *url.URL) ([]option.ClientOption, error) {
	query := url.Query()

	var options []option.ClientOption
	if apiKey := query.Get("apiKey"); apiKey != "" {
		options = append(options, option.WithAPIKey(apiKey))
	}
	if credentials := query.Get("credentials"); credentials != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(credentials)
		if err != nil {
			return nil, err
		}
		options = append(options, option.WithCredentialsJSON([]byte(decoded)))
	}
	if credentialsFile := query.Get("credentialsFile"); credentialsFile != "" {
		options = append(options, option.WithCredentialsFile(credentialsFile))
	}
	if scopes := query["scopes"]; scopes != nil {
		options = append(options, option.WithScopes(scopes...))
	}
	if endpoint := query.Get("endpoint"); endpoint != "" {
		options = append(options, option.WithEndpoint(endpoint))
	}
	if userAgent := query.Get("userAgent"); userAgent != "" {
		options = append(options, option.WithUserAgent(userAgent))
	}
	if disableAuth := query.Get("disableAuth"); disableAuth == "true" {
		options = append(options, option.WithoutAuthentication())
	}
	return options, nil
}
