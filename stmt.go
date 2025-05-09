package bigquery

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/http"

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

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	iterator, err := s.iterator(ctx, args)
	if err != nil {
		return nil, err
	}

	return &result{
		iterator: iterator,
	}, nil
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	iterator, err := s.iterator(ctx, args)
	if err != nil {
		return nil, err
	}

	return &rows{
		iterator: iterator,
	}, nil
}

func (s *stmt) iterator(ctx context.Context, args []driver.NamedValue) (*bigquery.RowIterator, error) {
	if s.conn.invalid {
		return nil, driver.ErrBadConn
	}

	query := s.buildQuery(args)
	s.conn.getQueryOpt(query)

	job, err := query.Run(ctx)
	if err != nil {
		if sessionError(s.conn.sessionID, err) {
			s.conn.invalid = true
		}
		return nil, err
	}
	s.conn.getJobOpt(job)

	if query.DryRun {
		return nil, nil
	}

	if sessionID := getSessionID(job); sessionID != "" {
		s.conn.sessionID = sessionID
	}

	return job.Read(ctx)
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
