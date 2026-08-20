package services_test

import (
	"testing"

	"github.com/collibra/data-access-go-sdk/services"
	"github.com/collibra/data-access-go-sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataSourceListFilter(t *testing.T) {
	search := "my search"
	otherSearch := "other search"
	parent := "parent-id"

	tests := []struct {
		name       string
		ops        []func(*services.DataSourceListOptions)
		wantFilter *types.DataSourceFilterInput
	}{
		{
			name:       "no options",
			wantFilter: nil,
		},
		{
			name:       "search only",
			ops:        []func(*services.DataSourceListOptions){services.WithDataSourceListSearch(&search)},
			wantFilter: &types.DataSourceFilterInput{Search: &search},
		},
		{
			name:       "filter only",
			ops:        []func(*services.DataSourceListOptions){services.WithDataSourceListFilter(&types.DataSourceFilterInput{Parent: &parent})},
			wantFilter: &types.DataSourceFilterInput{Parent: &parent},
		},
		{
			name: "search is merged into the filter",
			ops: []func(*services.DataSourceListOptions){
				services.WithDataSourceListFilter(&types.DataSourceFilterInput{Parent: &parent}),
				services.WithDataSourceListSearch(&search),
			},
			wantFilter: &types.DataSourceFilterInput{Parent: &parent, Search: &search},
		},
		{
			name: "search overrides the search of the filter",
			ops: []func(*services.DataSourceListOptions){
				services.WithDataSourceListFilter(&types.DataSourceFilterInput{Search: &otherSearch}),
				services.WithDataSourceListSearch(&search),
			},
			wantFilter: &types.DataSourceFilterInput{Search: &search},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := services.NewDataSourceListOptions(test.ops...)

			assert.Equal(t, test.wantFilter, services.DataSourceListFilter(&options))
		})
	}
}

func TestDataSourceListFilterDoesNotModifyTheGivenFilter(t *testing.T) {
	search := "my search"
	parent := "parent-id"
	given := &types.DataSourceFilterInput{Parent: &parent}

	options := services.NewDataSourceListOptions(
		services.WithDataSourceListFilter(given),
		services.WithDataSourceListSearch(&search),
	)
	filter := services.DataSourceListFilter(&options)

	require.NotNil(t, filter)
	assert.Equal(t, &search, filter.Search)
	assert.Nil(t, given.Search, "the filter passed by the caller should not be modified")
}
