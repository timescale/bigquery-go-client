package bigquery

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)

var (
	_ driver.Stmt             = (*stmt)(nil)
	_ driver.StmtExecContext  = (*stmt)(nil)
	_ driver.StmtQueryContext = (*stmt)(nil)
)

type stmt struct {
	conn  *conn
	query string
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, driver.ErrSkip
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, driver.ErrSkip
}

// ExecContext waits for the job's final status (rather than reading its
// results) so that RowsAffected doesn't need to make an API call.
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	job, err := s.run(ctx, args)
	if err != nil {
		return nil, err
	}
	// Dry run.
	if job == nil {
		return &result{}, nil
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, s.jobError(ctx, job, err)
	}
	if err := status.Err(); err != nil {
		s.checkSessionError(err)
		return nil, err
	}
	return newResult(status), nil
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	iterator, err := s.read(ctx, args)
	if err != nil {
		return nil, err
	}

	return &rows{
		iterator: iterator,
	}, nil
}

// Starts the query, returning a nil job for dry runs.
func (s *stmt) run(ctx context.Context, args []driver.NamedValue) (*bigquery.Job, error) {
	defer s.conn.options.reset()

	if s.conn.invalid {
		return nil, driver.ErrBadConn
	}

	query := s.buildQuery(args)
	s.conn.getQueryOpt(query)

	job, err := query.Run(ctx)
	if err != nil {
		s.checkSessionError(err)
		return nil, err
	}
	s.conn.getJobOpt(job)

	if query.DryRun {
		return nil, nil
	}

	if sessionID := getSessionID(job); sessionID != "" {
		s.conn.sessionID = sessionID
	}
	return job, nil
}

// Returns a nil iterator for dry runs.
func (s *stmt) read(ctx context.Context, args []driver.NamedValue) (*bigquery.RowIterator, error) {
	job, err := s.run(ctx, args)
	if err != nil || job == nil {
		return nil, err
	}

	iterator, err := job.Read(ctx)
	if err != nil {
		return nil, s.jobError(ctx, job, err)
	}
	return iterator, nil
}

// Handles an error from waiting for a running job.
func (s *stmt) jobError(ctx context.Context, job *bigquery.Job, err error) error {
	if ctx.Err() != nil {
		return errors.Join(err, cancelJob(ctx, job))
	}
	s.checkSessionError(err)
	return err
}

const cancelTimeout = 10 * time.Second

// Read/Wait stop waiting when ctx is done, but the job keeps running (and costing
// money) unless it is explicitly cancelled.
func cancelJob(ctx context.Context, job *bigquery.Job) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cancelTimeout)
	defer cancel()
	if err := job.Cancel(ctx); err != nil {
		return fmt.Errorf("error cancelling job: %w", err)
	}
	return nil
}

func getSessionID(job *bigquery.Job) string {
	status := job.LastStatus()
	if status == nil {
		return ""
	}
	if status.Statistics == nil {
		return ""
	}
	if status.Statistics.SessionInfo == nil {
		return ""
	}
	return status.Statistics.SessionInfo.SessionID
}

func (s *stmt) buildQuery(args []driver.NamedValue) *bigquery.Query {
	query := s.conn.client.Query(s.query)
	query.DefaultDatasetID = s.conn.config.Dataset
	query.Parameters = s.buildParameters(args)
	query.ConnectionProperties = s.buildConnectionProperties()
	query.CreateSession = s.conn.sessionID == ""

	return query
}

func (s *stmt) buildParameters(args []driver.NamedValue) []bigquery.QueryParameter {
	params := make([]bigquery.QueryParameter, len(args))
	for i, arg := range args {
		params[i] = bigquery.QueryParameter{
			Name:  arg.Name,
			Value: arg.Value,
		}
	}
	return params
}

func (s *stmt) buildConnectionProperties() []*bigquery.ConnectionProperty {
	if s.conn.sessionID == "" {
		return nil
	}
	return []*bigquery.ConnectionProperty{
		{
			Key:   "session_id",
			Value: s.conn.sessionID,
		},
	}
}

func (s *stmt) checkSessionError(err error) {
	if sessionError(s.conn.sessionID, err) {
		s.conn.invalid = true
	}
}

func sessionError(sessionID string, err error) bool {
	var bqErr *googleapi.Error
	if !errors.As(err, &bqErr) || bqErr.Code != http.StatusBadRequest {
		return false
	}

	sessionBrokenMsg := fmt.Sprintf("Session %s has expired and is no longer available.", sessionID)
	for _, errItem := range bqErr.Errors {
		if errItem.Reason == "resourcesExceeded" && errItem.Message == sessionBrokenMsg {
			return true
		}
	}
	return false
}
