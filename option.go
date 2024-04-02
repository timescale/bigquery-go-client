package bigquery

import (
	"database/sql/driver"

	"cloud.google.com/go/bigquery"
)

// Aliases for underlying cloud.google.com/go/bigquery types, for convenience.
type (
	Job   = bigquery.Job
	Query = bigquery.Query
)

// GetJob is a type of function that can be passed as an argument to a Query or
// Exec method to get a handle on the BigQuery job for the query (e.g. to get
// its statistics after the query completes). The function will be called with
// a *Job value before the Query/Exec method returns.
type GetJob func(job *Job)

// GetQuery is a type of function that can be passed as an argument to a Query
// or Exec method to get a handle on the BigQuery query before it's executed
// (e.g. to execute a dry run). The function will be called with a *Query value
// before the Query/Exec method returns.
type GetQuery func(query *Query)

type options struct {
	getQuery GetQuery
	getJob   GetJob
}

func (o *options) CheckNamedValue(named *driver.NamedValue) error {
	switch value := named.Value.(type) {
	case GetQuery:
		o.getQuery = value
		return driver.ErrRemoveArgument
	case GetJob:
		o.getJob = value
		return driver.ErrRemoveArgument
	}
	return driver.ErrSkip
}

func (o *options) getQueryOpt(query *bigquery.Query) {
	if o.getQuery != nil {
		o.getQuery(query)
		o.getQuery = nil
	}
}

func (o *options) getJobOpt(job *bigquery.Job) {
	if o.getJob != nil {
		o.getJob(job)
		o.getJob = nil
	}
}
