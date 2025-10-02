package rest

import (
	"net/http"

	"github.com/Khan/genqlient/graphql"
)

var _ http.RoundTripper = (*DoerRoundTripWrapper)(nil)

type DoerRoundTripWrapper struct {
	Doer graphql.Doer
}

func (d DoerRoundTripWrapper) RoundTrip(request *http.Request) (*http.Response, error) {
	return d.Doer.Do(request)
}
