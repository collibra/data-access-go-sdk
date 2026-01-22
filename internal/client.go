package internal

import (
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/go-retryablehttp"
)

type ClientOptions struct {
	// Auth
	Username string
	Password string

	// MaxRetries specifies the maximum number of retries for failed requests.
	RetryWaitMin time.Duration
	RetryWaitMax time.Duration
	RetryMax     int

	Backoff retryablehttp.Backoff

	UserAgent string
}

func CreateHttpClient(options *ClientOptions) *http.Client {
	// 1. Create clean http transport
	transport := cleanhttp.DefaultPooledTransport()

	// 2. Wrap it with an auth round tripper
	var authRoundTripper http.RoundTripper
	if options.Username == "" && options.Password == "" {
		authRoundTripper = transport
	} else {
		authRoundTripper = &BasicAuthRoundTripper{
			User:     options.Username,
			Password: options.Password,
			Proxied:  transport,
		}
	}

	// 3. Create a retryable http client
	retryableClient := &retryablehttp.Client{
		HTTPClient: &http.Client{
			Transport: authRoundTripper,
		},
		Logger:       nil,
		RetryWaitMin: options.RetryWaitMin,
		RetryWaitMax: options.RetryWaitMax,
		RetryMax:     options.RetryMax,
		CheckRetry:   retryablehttp.DefaultRetryPolicy,
		Backoff:      options.Backoff,
	}

	retryableTransport := retryablehttp.RoundTripper{
		Client: retryableClient,
	}

	// 4. Wrap it with SDK header transport
	sdkHeaderTransport := &SdkHeaderTransport{
		UserAgent: options.UserAgent,
		Proxied:   &retryableTransport,
	}

	return &http.Client{
		Transport: sdkHeaderTransport,
	}
}

type SdkHeaderTransport struct {
	Proxied http.RoundTripper

	UserAgent string
	once      sync.Once
}

func (t *SdkHeaderTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.once.Do(func() {
		if t.UserAgent == "" {
			// Set default User-Agent if not provided
			t.UserAgent = fmt.Sprintf("Collibra Data Access SDK/%s", t.GetVersion())
		}
	})

	req.Header.Set("User-Agent", t.UserAgent)

	return t.Proxied.RoundTrip(req) //nolint:wrapcheck
}

const myModulePath = "github.com/collibra/data-access-go-sdk"

// GetVersion returns the version of the library.
// It returns "unknown" if the version cannot be found.
func (t *SdkHeaderTransport) GetVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		// This can happen if the binary was built without
		// module support (e.g., with GO111MODULE=off).
		return "unknown (no build info)"
	}

	// Loop through the dependencies to find our own module
	for _, dep := range info.Deps {
		if dep.Path == myModulePath {
			return dep.Version
		}
	}

	// Also check the main module, in case the library
	// is being run as the main program (e.g., for tests).
	if info.Main.Path == myModulePath {
		return info.Main.Version
	}

	// If not found, it might be a local replace or other
	// non-standard build.
	return "unknown (not found in deps)"
}
