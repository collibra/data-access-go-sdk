package internal

import (
	"net/http"
)

type BasicAuthRoundTripper struct {
	User     string
	Password string //nolint:gosec

	Proxied http.RoundTripper
}

func (a *BasicAuthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.SetBasicAuth(a.User, a.Password)

	return a.Proxied.RoundTrip(req) //nolint:wrapcheck
}
