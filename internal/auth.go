package internal

import (
	"fmt"
	"net/http"
)

type BasicAuthedDoer struct {
	User     string
	Password string
	Url      string
}

func (a *BasicAuthedDoer) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", "Collibra Data Access SDK")
	req.SetBasicAuth(a.User, a.Password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error while doing HTTP POST to %q: %s", req.URL.String(), err.Error())
	}

	return resp, nil
}
