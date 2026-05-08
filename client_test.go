package sdk

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"
)

type NewClientTestSuite struct {
	suite.Suite
}

func TestNewClientTestSuite(t *testing.T) {
	suite.Run(t, new(NewClientTestSuite))
}

func (s *NewClientTestSuite) TestEmptyURL() {
	_, err := NewClient("")
	s.Require().Error(err)
	s.Contains(err.Error(), "url must not be empty")
}

func (s *NewClientTestSuite) TestRelativeURL() {
	_, err := NewClient("/dataAccess/query")
	s.Require().Error(err)
}

func (s *NewClientTestSuite) TestMissingHost() {
	_, err := NewClient("http://")
	s.Require().Error(err)
}

// TestDataAccessPathAppended verifies that the /dataAccess/ segment is appended
// when the provided URL does not already include it.
func (s *NewClientTestSuite) TestDataAccessPathAppended() {
	capturedPath := s.captureRequestPath(func(serverURL string) {
		client, err := NewClient(serverURL, WithRetryMax(0))
		s.Require().NoError(err)

		_, _ = client.User().GetCurrentUser(context.Background())
	})
	s.Equal("/dataAccess/query", capturedPath)
}

// TestDataAccessPathNotDoubledWithTrailingSlash verifies that providing a URL
// that already ends with /dataAccess/ does not result in the path being appended again.
func (s *NewClientTestSuite) TestDataAccessPathNotDoubledWithTrailingSlash() {
	capturedPath := s.captureRequestPath(func(serverURL string) {
		client, err := NewClient(serverURL+"/dataAccess/", WithRetryMax(0))
		s.Require().NoError(err)

		_, _ = client.User().GetCurrentUser(context.Background())
	})
	s.Equal("/dataAccess/query", capturedPath)
}

// TestDataAccessPathNotDoubledWithoutTrailingSlash verifies that providing a URL
// that already ends with /dataAccess (no trailing slash) does not result in doubling.
func (s *NewClientTestSuite) TestDataAccessPathNotDoubledWithoutTrailingSlash() {
	capturedPath := s.captureRequestPath(func(serverURL string) {
		client, err := NewClient(serverURL+"/dataAccess", WithRetryMax(0))
		s.Require().NoError(err)

		_, _ = client.User().GetCurrentUser(context.Background())
	})
	s.Equal("/dataAccess/query", capturedPath)
}

func (s *NewClientTestSuite) captureRequestPath(fn func(serverURL string)) string {
	var capturedPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errors":[{"message":"test server"}]}`))
	}))
	defer server.Close()

	fn(server.URL)

	return capturedPath
}
