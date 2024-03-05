# BigQuery Driver

A [database/sql](https://pkg.go.dev/database/sql) driver for BigQuery.

## Features

- Implements all modern [database/sql/driver](https://pkg.go.dev/database/sql/driver) interfaces.
- Supports query cancellation and timeouts via [context.Context](https://pkg.go.dev/context).
- Supports sessions (each [sql.Conn](https://pkg.go.dev/database/sql#Conn) maps
  to a single [BigQuery session](https://cloud.google.com/bigquery/docs/sessions-intro)).
- Compliant with the [database/sql](https://pkg.go.dev/database/sql) package
  interface. In particular, only valid [driver.Value](https://pkg.go.dev/database/sql/driver#Value)
  types are returned, and the driver therefore behaves as documented in the
  [Rows.Scan](https://pkg.go.dev/database/sql#Rows.Scan) documentation.

## DSN

To connect via the [sql.Open](https://pkg.go.dev/database/sql#Open) method, use
`bigquery` as the driver name and a DSN (Data Source Name) of the following form:

```
bigquery://projectID[/location][/dataset]?key=val
```

Both the `location` and `dataset` are optional.

Credentials can be passed via the `GOOGLE_APPLICATION_CREDENTIALS` environment
variable (see [Application Default
Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
for more information).

Alternatively, you can configure authentication via one of the `apiKey`,
`credentials`, or `credentialsFile` options (see below).

### Options

Options can be passed as key/value pairs in the query string. Supported options
include:

- `apiKey` - API Key to be used for authentication.
- `credentials` - Base64-encoded service account or refresh token credentials
  JSON object.
- `credentialsFile` - Path to a file containing a service account or refresh
  token credentials JSON object.
- `scopes` - Overrides the default OAuth2 scopes to be used.
- `endpoint` - Overrides the default endpoint to be used.
- `userAgent` - Sets the User-Agent that is used when making requests to the
  BigQuery API.
- `disableAuth` - Set to `true` to disable all authentication methods. Primarily
  useful in testing, or when accessing publicly accessible resources.

If you would like any other [option.ClientOption](https://pkg.go.dev/google.golang.org/api/option#ClientOption)
options to be supported via the DSN, feel free to a pull request or submit an
issue.

### Example

```go
package main

import (
	"database/sql"

	_ "github.com/popsql/bigquery-go-client"
)

var db, _ = sql.Open("bigquery", "bigquery://PROJECT_ID/LOCATION/DATASET?credentialsFile=/path/to/credentials.json")
```

## Connector

The [NewConnector](https://pkg.go.dev/github.com/popsql/bigquery-go-client#NewConnector)
method can be used to create a [driver.Connector](https://pkg.go.dev/database/sql/driver#Connector)
instance which can be passed to [sql.OpenDB](https://pkg.go.dev/database/sql#OpenDB).
This provides a more flexible and powerful way of opening a [sql.DB](https://pkg.go.dev/database/sql#DB)
instance, and supports all of the available [option.ClientOption](https://pkg.go.dev/google.golang.org/api/option#ClientOption)
options.

### Example

```go
package main

import (
	"database/sql"

	bigquery "github.com/popsql/bigquery-go-client"
	"google.golang.org/api/option"
)

var db = sql.OpenDB(bigquery.NewConnector(bigquery.Config{
	ProjectID: "PROJECT_ID",
	Dataset:   "DATASET",
	Location:  "LOCATION",
	Options: []option.ClientOption{
		option.WithCredentialsFile("/path/to/credentials.json"),
	},
}))
```

## Data Types

The driver supports all [BigQuery data types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types),
which are scanned into the following Go types by default:

| BigQuery Type | Go Type |
| ------------- | ------- |
| STRING | string |
| BYTES | []byte |
| INT64 | int64 |
| FLOAT64 | float64 |
| BOOL | bool |
| TIMESTAMP | time.Time |
| DATE | string |
| TIME | string |
| DATETIME | string |
| NUMERIC | string |
| BIGNUMERIC | string |
| GEOGRAPHY | string |
| INTERVAL | string |
| RANGE | string |
| JSON | []byte |
| ARRAY | []byte |
| STRUCT | []byte |

Note that several types are returned as `string`/`[]byte` values instead of as
the more specific types that the underlying [bigquery.RowIterator.Next](https://pkg.go.dev/cloud.google.com/go/bigquery#RowIterator.Next)
method returns by default. While this may seem inconvenient, it is necessary to
satisfy the [driver.Value](https://pkg.go.dev/database/sql/driver#Value)
requirements, which only allow a narrow range of predefined types. Adhering to
these requirements ensures that [sql.Rows.Scan](https://pkg.go.dev/database/sql#Rows.Scan)
always functions as described in the documentation.

Note that the `ARRAY` and `STRUCT` types are returned as a `[]byte` value
containing a JSON representation of the `ARRAY`/`STRUCT`. `STRUCT` types are
marshalled as JSON objects, where each field is represented as a key/value in
the JSON object. It should therefore be possible to call
[json.Unmarshal](https://pkg.go.dev/encoding/json#Unmarshal) to convert the
`[]byte` returned into a Go map, slice, or struct of your choosing.

To scan directly into a more complex type, create your own
[sql.Scanner](https://pkg.go.dev/database/sql#Scanner) implementation (for
example, one that wraps one of the [civil](https://pkg.go.dev/cloud.google.com/go/civil)
types, for `DATE`/`TIME`/`DATETIME`). Such types might be added to this package in
the future.
