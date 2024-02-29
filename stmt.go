package driver

import (
	"context"
	"database/sql/driver"

	"cloud.google.com/go/bigquery"
)

var (
	_ driver.Stmt             = (*stmt)(nil)
	_ driver.StmtExecContext  = (*stmt)(nil)
	_ driver.StmtQueryContext = (*stmt)(nil)
)

type stmt struct {
	connection *conn
	query      string
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
	query := s.buildQuery(args)

	if query.CreateSession {
		job, err := query.Run(ctx)
		if err != nil {
			return nil, err
		}

		s.connection.sessionID = job.LastStatus().Statistics.SessionInfo.SessionID
		return job.Read(ctx)
	}

	return query.Read(ctx)
}

func (s *stmt) buildQuery(args []driver.NamedValue) *bigquery.Query {
	query := s.connection.client.Query(s.query)
	query.DefaultProjectID = s.connection.config.ProjectID
	query.DefaultDatasetID = s.connection.config.Dataset
	query.Parameters = s.buildParameters(args)
	query.ConnectionProperties = s.buildConnectionProperties()
	query.CreateSession = s.connection.sessionID == ""

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
	if s.connection.sessionID == "" {
		return nil
	}
	return []*bigquery.ConnectionProperty{
		{
			Key:   "session_id",
			Value: s.connection.sessionID,
		},
	}
}
