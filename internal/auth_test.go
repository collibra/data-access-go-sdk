package internal

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"
)

type AuthTestSuite struct {
	suite.Suite
}

func TestAuthTestSuite(t *testing.T) {
	suite.Run(t, new(AuthTestSuite))
}

func (suite *AuthTestSuite) TestBasicAuthRoundTripper_RoundTrip() {
	var capturedUsername, capturedPassword string
	var authPresent bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedUsername, capturedPassword, authPresent = r.BasicAuth()

		w.WriteHeader(http.StatusOK)

		_, err := w.Write([]byte("OK"))
		if err != nil {
			// Handle the error (log it, return it, etc.)
			suite.T().Logf("failed to write header: %v", err)
		}
	}))
	defer server.Close()

	authRoundTripper := &BasicAuthRoundTripper{
		User:     "testuser",
		Password: "testpassword",
		Proxied:  http.DefaultTransport,
	}

	client := &http.Client{
		Transport: authRoundTripper,
	}

	resp, err := client.Get(server.URL)
	suite.Require().NoError(err)

	suite.Require().NotNil(resp)
	defer resp.Body.Close()

	suite.True(authPresent, "Expected Basic Auth to be present")
	suite.Equal("testuser", capturedUsername)
	suite.Equal("testpassword", capturedPassword)
	suite.Equal(http.StatusOK, resp.StatusCode)
}

func (suite *AuthTestSuite) TestBasicAuthRoundTripper_PreservesHeaders() {
	var capturedHeaders http.Header

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	authRoundTripper := &BasicAuthRoundTripper{
		User:     "user",
		Password: "pass",
		Proxied:  http.DefaultTransport,
	}

	client := &http.Client{
		Transport: authRoundTripper,
	}

	req, err := http.NewRequest(http.MethodPost, server.URL, http.NoBody)
	suite.Require().NoError(err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	suite.Require().NoError(err)

	defer resp.Body.Close()

	suite.Equal("application/json", capturedHeaders.Get("Content-Type"))
	suite.NotEmpty(capturedHeaders.Get("Authorization"))
}
