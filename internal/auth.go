package internal

import (
	"net/http"
)

type BasicAuthRoundTripper struct {
	User     string
	Password string

	Proxied http.RoundTripper
}

func (a *BasicAuthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", "Collibra Data Access SDK")
	req.SetBasicAuth(a.User, a.Password)

	return a.Proxied.RoundTrip(req)
}
