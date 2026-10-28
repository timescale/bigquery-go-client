package bigquery

import (
	"encoding/base64"
	"errors"
	"reflect"
	"testing"

	"google.golang.org/api/option"
)

func TestParseDSN(t *testing.T) {
	creds := []byte(`{"type":"service_account"}`)
	encodedCreds := base64.RawURLEncoding.EncodeToString(creds)

	tests := []struct {
		name    string
		dsn     string
		want    Config
		wantErr bool
	}{
		{
			name: "project only",
			dsn:  "bigquery://my-project",
			want: Config{ProjectID: "my-project"},
		},
		{
			name: "project and dataset",
			dsn:  "bigquery://my-project/my_dataset",
			want: Config{ProjectID: "my-project", Dataset: "my_dataset"},
		},
		{
			name: "project, location, and dataset",
			dsn:  "bigquery://my-project/US/my_dataset",
			want: Config{ProjectID: "my-project", Location: "US", Dataset: "my_dataset"},
		},
		{
			name: "trailing slash",
			dsn:  "bigquery://my-project/US/my_dataset/",
			want: Config{ProjectID: "my-project", Location: "US", Dataset: "my_dataset"},
		},
		{
			name: "all options",
			dsn: "bigquery://my-project/my_dataset" +
				"?apiKey=key" +
				"&credentialsType=service_account" +
				"&credentials=" + encodedCreds +
				"&credentialsFile=/path/to/creds.json" +
				"&scopes=scope1&scopes=scope2" +
				"&endpoint=http://localhost:9050" +
				"&userAgent=agent" +
				"&disableAuth=true",
			want: Config{
				ProjectID: "my-project",
				Dataset:   "my_dataset",
				Options: []option.ClientOption{
					option.WithAPIKey("key"),
					option.WithAuthCredentialsJSON(option.ServiceAccount, creds),
					option.WithAuthCredentialsFile(option.ServiceAccount, "/path/to/creds.json"),
					option.WithScopes("scope1", "scope2"),
					option.WithEndpoint("http://localhost:9050"),
					option.WithUserAgent("agent"),
					option.WithoutAuthentication(),
				},
			},
		},
		{
			name: "disableAuth not true",
			dsn:  "bigquery://my-project?disableAuth=false",
			want: Config{ProjectID: "my-project"},
		},
		{
			name:    "invalid URL",
			dsn:     "bigquery://my-project/%zz",
			wantErr: true,
		},
		{
			name:    "invalid scheme",
			dsn:     "postgres://my-project",
			wantErr: true,
		},
		{
			name:    "too many path segments",
			dsn:     "bigquery://my-project/US/my_dataset/extra",
			wantErr: true,
		},
		{
			name:    "credentials without credentialsType",
			dsn:     "bigquery://my-project?credentials=" + encodedCreds,
			wantErr: true,
		},
		{
			name:    "credentialsFile without credentialsType",
			dsn:     "bigquery://my-project?credentialsFile=/path/to/creds.json",
			wantErr: true,
		},
		{
			name:    "credentials not base64",
			dsn:     "bigquery://my-project?credentialsType=service_account&credentials=!!!",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseDSN(tt.dsn)
			if tt.wantErr {
				var connStrErr *invalidConnStrError
				if !errors.As(err, &connStrErr) {
					t.Fatalf("parseDSN() error = %v, want *invalidConnStrError", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseDSN() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseDSN() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
