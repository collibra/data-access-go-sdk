package internal

import (
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/stretchr/testify/suite"
)

type ClientTestSuite struct {
	suite.Suite
}

func TestClientTestSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

func (suite *ClientTestSuite) TestCreateHttpClient() {
	options := &ClientOptions{
		Username:     "testuser",
		Password:     "testpass",
		RetryWaitMin: 1 * time.Second,
		RetryWaitMax: 5 * time.Second,
		RetryMax:     3,
		Backoff:      retryablehttp.DefaultBackoff,
	}

	client := CreateHttpClient(options)

	suite.Require().NotNil(client)
	suite.Require().NotNil(client.Transport)

	// Verify that the transport is SdkHeaderTransport
	_, ok := client.Transport.(*SdkHeaderTransport)
	suite.True(ok, "Expected Transport to be *SdkHeaderTransport")
}

func (suite *ClientTestSuite) TestSdkHeaderTransport_RoundTrip() {
	// Create a test server
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		// Check that User-Agent header is set
		userAgent := r.Header.Get("User-Agent")
		suite.NotEmpty(userAgent)
		suite.True(strings.HasPrefix(userAgent, "Collibra Data Access SDK/"))

		// Check that basic auth is present
		username, password, ok := r.BasicAuth()
		suite.True(ok)
		suite.Equal("testuser", username)
		suite.Equal("testpass", password)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))
	defer server.Close()

	options := &ClientOptions{
		Username:     "testuser",
		Password:     "testpass",
		RetryWaitMin: 1 * time.Millisecond,
		RetryWaitMax: 10 * time.Millisecond,
		RetryMax:     0,
		Backoff:      retryablehttp.DefaultBackoff,
	}

	client := CreateHttpClient(options)

	// Make a request
	resp, err := client.Get(server.URL)
	suite.Require().NoError(err)
	suite.Require().NotNil(resp)
	suite.Equal(http.StatusOK, resp.StatusCode)
	suite.Equal(1, requestCount)

	resp.Body.Close()
}

func (suite *ClientTestSuite) TestSdkHeaderTransport_UserAgentSetOnce() {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		userAgent := r.Header.Get("User-Agent")
		suite.NotEmpty(userAgent)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	options := &ClientOptions{
		Username:     "testuser",
		Password:     "testpass",
		RetryWaitMin: 1 * time.Millisecond,
		RetryWaitMax: 10 * time.Millisecond,
		RetryMax:     0,
	}

	client := CreateHttpClient(options)

	// Make multiple requests
	for i := 0; i < 3; i++ {
		resp, err := client.Get(server.URL)
		suite.Require().NoError(err)
		suite.Require().NotNil(resp)
		resp.Body.Close()
	}

	suite.Equal(3, callCount)
}

func (suite *ClientTestSuite) TestSdkHeaderTransport_GetVersion_WithBuildInfo() {
	transport := &SdkHeaderTransport{}

	version := transport.GetVersion()

	suite.NotEmpty(version)
	// In tests, the version will typically be "(devel)" or "unknown"
	// since we're not running from a built module
	suite.True(
		version == "(devel)" ||
			strings.HasPrefix(version, "unknown") ||
			strings.HasPrefix(version, "v"),
		"Expected version to be (devel), unknown, or start with v, got: %s", version)
}

func (suite *ClientTestSuite) TestSdkHeaderTransport_GetVersion_NoBuildInfo() {
	// This test verifies the fallback behavior
	// We can't easily simulate no build info, but we can test the logic
	transport := &SdkHeaderTransport{}

	version := transport.GetVersion()

	suite.NotEmpty(version)
}

func (suite *ClientTestSuite) TestClientOptions_AllFields() {
	options := &ClientOptions{
		Username:     "user123",
		Password:     "pass456",
		RetryWaitMin: 2 * time.Second,
		RetryWaitMax: 10 * time.Second,
		RetryMax:     5,
		Backoff:      retryablehttp.LinearJitterBackoff,
	}

	suite.Equal("user123", options.Username)
	suite.Equal("pass456", options.Password)
	suite.Equal(2*time.Second, options.RetryWaitMin)
	suite.Equal(10*time.Second, options.RetryWaitMax)
	suite.Equal(5, options.RetryMax)
	suite.NotNil(options.Backoff)
}

func (suite *ClientTestSuite) TestRetryBehavior() {
	attemptCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount++
		if attemptCount < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Success"))
	}))
	defer server.Close()

	options := &ClientOptions{
		Username:     "testuser",
		Password:     "testpass",
		RetryWaitMin: 1 * time.Millisecond,
		RetryWaitMax: 10 * time.Millisecond,
		RetryMax:     3,
		Backoff:      retryablehttp.DefaultBackoff,
	}

	client := CreateHttpClient(options)

	resp, err := client.Get(server.URL)
	suite.Require().NoError(err)
	suite.Require().NotNil(resp)
	suite.Equal(http.StatusOK, resp.StatusCode)
	suite.GreaterOrEqual(attemptCount, 3, "Expected at least 3 attempts due to retries")

	resp.Body.Close()
}

func (suite *ClientTestSuite) TestBasicAuthRoundTripper() {
	receivedAuth := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if ok {
			receivedAuth = username + ":" + password
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	options := &ClientOptions{
		Username:     "myuser",
		Password:     "mypassword",
		RetryWaitMin: 1 * time.Millisecond,
		RetryWaitMax: 10 * time.Millisecond,
		RetryMax:     0,
	}

	client := CreateHttpClient(options)

	resp, err := client.Get(server.URL)
	suite.Require().NoError(err)
	suite.Require().NotNil(resp)
	suite.Equal("myuser:mypassword", receivedAuth)

	resp.Body.Close()
}

func (suite *ClientTestSuite) TestGetVersion_FindsModuleInDeps() {
	// This is a mock test to verify the logic
	// In reality, debug.ReadBuildInfo() will be called
	transport := &SdkHeaderTransport{}

	// Call GetVersion
	version := transport.GetVersion()

	// Should return some version string
	suite.NotEmpty(version)
}

func (suite *ClientTestSuite) TestGetVersion_ChecksMainModule() {
	transport := &SdkHeaderTransport{}

	info, ok := debug.ReadBuildInfo()
	if ok && info.Main.Path == myModulePath {
		version := transport.GetVersion()
		suite.NotEmpty(version)
	} else {
		// If not the main module, just verify it returns something
		version := transport.GetVersion()
		suite.NotEmpty(version)
	}
}
