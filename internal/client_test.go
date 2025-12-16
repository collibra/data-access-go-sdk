package internal

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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
    var requestCount int32 

    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        atomic.AddInt32(&requestCount, 1) // Atomic increment

        userAgent := r.Header.Get("User-Agent")
        suite.NotEmpty(userAgent)
        suite.True(strings.HasPrefix(userAgent, "Collibra Data Access SDK/"))

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
    resp, err := client.Get(server.URL)
    suite.Require().NoError(err)
    resp.Body.Close()

    suite.Equal(int32(1), atomic.LoadInt32(&requestCount))
}

func (suite *ClientTestSuite) TestRetryBehavior() {
    var attemptCount int32

    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        current := atomic.AddInt32(&attemptCount, 1)
        if current < 3 {
            w.WriteHeader(http.StatusServiceUnavailable)
            return
        }
        w.WriteHeader(http.StatusOK)
    }))
    defer server.Close()

    options := &ClientOptions{
        RetryWaitMin: 1 * time.Millisecond,
        RetryWaitMax: 5 * time.Millisecond, 
        RetryMax:     3,
        Backoff:      retryablehttp.DefaultBackoff,
    }

    client := CreateHttpClient(options)
    resp, err := client.Get(server.URL)
    
    suite.Require().NoError(err)
    suite.Equal(http.StatusOK, resp.StatusCode)
    
    suite.GreaterOrEqual(atomic.LoadInt32(&attemptCount), int32(3))
    resp.Body.Close()
}

func (suite *ClientTestSuite) TestSdkHeaderTransport_GetVersion_Sanity() {
    transport := &SdkHeaderTransport{}
    version := transport.GetVersion()

    suite.NotEmpty(version)
}
