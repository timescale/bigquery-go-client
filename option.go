package bigquery

import (
	"database/sql/driver"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

// GetJob is a type of function that can be passed as an argument to a Query or
// Exec method to get a handle on the BigQuery job for the query (e.g. to get
// its statistics after the query completes). The function will be called with
// a [bigquery.Job] value before the Query/Exec method returns.
type GetJob func(job *bigquery.Job)

// GetQuery is a type of function that can be passed as an argument to a Query
// or Exec method to get a handle on the BigQuery query before it's executed
// (e.g. to execute a dry run). The function will be called with a
// [bigquery.Query] value before the Query/Exec method returns.
type GetQuery func(query *bigquery.Query)

type options struct {
	getQuery GetQuery
	getJob   GetJob
}

// CheckNamedValue extracts GetQuery/GetJob options, and otherwise converts
// arguments using the database/sql defaults where possible. Non-Valuer
// arguments the defaults can't handle (e.g. *big.Rat, slices, structs) are
// passed through as-is, since the BigQuery client supports them as query
// parameters (and reports an error at query time if it doesn't). The civil
// types are passed through too, since their Valuer implementations would turn
// them into STRING parameters.
func (o *options) CheckNamedValue(named *driver.NamedValue) error {
	switch value := named.Value.(type) {
	case GetQuery:
		o.getQuery = value
		return driver.ErrRemoveArgument
	case GetJob:
		o.getJob = value
		return driver.ErrRemoveArgument
	case civil.Date, civil.Time, civil.DateTime:
		return nil
	case driver.Valuer:
		return driver.ErrSkip
	}

	if value, err := driver.DefaultParameterConverter.ConvertValue(named.Value); err == nil {
		named.Value = value
	}
	return nil
}

func (o *options) getQueryOpt(query *bigquery.Query) {
	if o.getQuery != nil {
		o.getQuery(query)
	}
}

func (o *options) getJobOpt(job *bigquery.Job) {
	if o.getJob != nil {
		o.getJob(job)
	}
}

// Options are set on the conn by CheckNamedValue, so they must be cleared after
// each query (even a failed one) to avoid leaking into later queries.
func (o *options) reset() {
	*o = options{}
}
