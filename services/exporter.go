package services

import (
	"context"
	"fmt"
	"iter"
	"net/http"

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

func (c *ExporterClient) Export(ctx context.Context, dataSourceId string) iter.Seq2[types.ExportedAccessControl, error] {
	return func(yield func(types.ExportedAccessControl, error) bool) {
		resp, err := c.client.Get(ctx, "access-control/export/"+dataSourceId)
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
