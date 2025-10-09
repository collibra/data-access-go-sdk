package services

import (
	"context"
	"fmt"
	"iter"
	"net/http"
	"net/url"

	"gopkg.in/yaml.v3"

	"github.com/collibra/access-governance-go-sdk/internal/rest"
	"github.com/collibra/access-governance-go-sdk/types"
)

type ExporterClient struct {
	client *rest.RestClient
}

func NewExporterClient(client *rest.RestClient) *ExporterClient {
	return &ExporterClient{
		client: client,
	}
}

type ExportOptions struct {
	OutOfSyncOnly bool
}

func WithExportOutOfSyncOnly() func(options *ExportOptions) {
	return func(options *ExportOptions) {
		options.OutOfSyncOnly = true
	}
}

func (c *ExporterClient) Export(ctx context.Context, dataSourceId string, ops ...func(options *ExportOptions)) iter.Seq2[types.ExportedAccessControl, error] {
	options := &ExportOptions{}

	for _, op := range ops {
		op(options)
	}

	return func(yield func(types.ExportedAccessControl, error) bool) {
		path, err := url.JoinPath("access-control/export/", dataSourceId)
		if err != nil {
			yield(types.ExportedAccessControl{}, fmt.Errorf("join path: %w", err))
		}

		resp, err := c.client.Get(ctx, path, func(r *http.Request) {
			if options.OutOfSyncOnly {
				q := r.URL.Query()
				q.Set("notSynced", "true")
				r.URL.RawQuery = q.Encode()
			}
		})
		if err != nil {
			yield(types.ExportedAccessControl{}, fmt.Errorf("request: %w", err))

			return
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			yield(types.ExportedAccessControl{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode))

			return
		}

		output := struct {
			LastCalculated int64                         `yaml:"lastCalculated"`
			AccessControls []types.ExportedAccessControl `yaml:"accessControls"`
		}{}

		if err = yaml.NewDecoder(resp.Body).Decode(&output); err != nil {
			yield(types.ExportedAccessControl{}, fmt.Errorf("decode response: %w", err))

			return
		}

		for idx := range output.AccessControls {
			if !yield(output.AccessControls[idx], nil) {
				return
			}
		}
	}
}
