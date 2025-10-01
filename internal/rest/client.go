package rest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type RestClient struct {
	basePath string
	client   *http.Client
}

func NewRestClient(basePath string, client *http.Client) *RestClient {
	return &RestClient{
		basePath: basePath,
		client:   client,
	}
}

func (c *RestClient) Do(ctx context.Context, method string, path string, ops ...func(r *http.Request)) (*http.Response, error) {
	urlPath, err := url.JoinPath(c.basePath, path)
	if err != nil {
		return nil, fmt.Errorf("join path: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, urlPath, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for _, op := range ops {
		op(req)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}

	return resp, nil
}

func (c *RestClient) Get(ctx context.Context, path string, ops ...func(r *http.Request)) (*http.Response, error) {
	return c.Do(ctx, http.MethodGet, path, ops...)
}

func (c *RestClient) Head(ctx context.Context, path string, ops ...func(r *http.Request)) (*http.Response, error) {
	return c.Do(ctx, http.MethodHead, path, ops...)
}

func (c *RestClient) Post(ctx context.Context, path string, contentType string, body io.Reader, ops ...func(r *http.Request)) (*http.Response, error) {
	ops = append([]func(r *http.Request){
		func(r *http.Request) {
			r.Header.Set("Content-Type", contentType)
			r.Body = io.NopCloser(body)
		},
	}, ops...)

	return c.Do(ctx, http.MethodPost, path, ops...)
}

func (c *RestClient) PostForm(ctx context.Context, path string, data url.Values, ops ...func(r *http.Request)) (*http.Response, error) {
	return c.Post(ctx, path, "application/x-www-form-urlencoded", io.NopCloser(io.Reader(strings.NewReader(data.Encode()))), ops...)
}
