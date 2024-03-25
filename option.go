package bigquery

import (
	"errors"

	"cloud.google.com/go/bigquery"
)

type jobStatsOpt struct {
	stats **bigquery.JobStatistics
}

// GetJobStatistics can be passed as an argument to a Query or Exec method to
// get back the job's statistics from BigQuery. The **bigquery.JobStatistics
// parameter must be non-nil. It will be populated with a *bigquery.JobStatistics
// value by the time the Query/Exec method returns.
func GetJobStatistics(stats **bigquery.JobStatistics) jobStatsOpt {
	if stats == nil {
		panic(errors.New("WithJobStatistics expects non-nil *bigquery.JobStatistics parameter"))
	}
	return jobStatsOpt{stats: stats}
}
