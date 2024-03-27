package bigquery

import (
	"errors"

	"cloud.google.com/go/bigquery"
)

type Job = *bigquery.Job

type jobOpt struct {
	job *Job
}

// GetJob can be passed as an argument to a Query or Exec method to get a
// handle on the BigQuery job for the query (e.g. to get its statistics after
// the query completes). The *Job parameter must be non-nil. It will be
// populated with a Job value by the time the Query/Exec method returns.
func GetJob(job *Job) jobOpt {
	if job == nil {
		panic(errors.New("GetJob expects non-nil *Job parameter"))
	}
	return jobOpt{job: job}
}
