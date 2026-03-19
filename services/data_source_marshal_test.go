package services_test

import (
	"testing"

	"github.com/collibra/data-access-go-sdk/services"
	"github.com/collibra/data-access-go-sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalSyncParameterValues(t *testing.T) {
	tests := []struct {
		name          string
		input         types.SyncParameterValuesInput
		wantValues    []types.SyncParameterValueInput
		wantErrSubstr string
	}{
		{
			name: "bool true",
			input: types.SyncParameterValuesInput{
				DataSourceId: "ds-1",
				Values:       []types.SyncParameterValueInput{{Path: "a.b", Value: new(any(true))}},
			},
			wantValues: []types.SyncParameterValueInput{{Path: "a.b", Value: new(any("true"))}},
		},
		{
			name: "bool false",
			input: types.SyncParameterValuesInput{
				DataSourceId: "ds-1",
				Values:       []types.SyncParameterValueInput{{Path: "a.b", Value: new(any(false))}},
			},
			wantValues: []types.SyncParameterValueInput{{Path: "a.b", Value: new(any("false"))}},
		},
		{
			name: "integer",
			input: types.SyncParameterValuesInput{
				DataSourceId: "ds-1",
				Values:       []types.SyncParameterValueInput{{Path: "a.b", Value: new(any(42))}},
			},
			wantValues: []types.SyncParameterValueInput{{Path: "a.b", Value: new(any("42"))}},
		},
		{
			name: "float",
			input: types.SyncParameterValuesInput{
				DataSourceId: "ds-1",
				Values:       []types.SyncParameterValueInput{{Path: "a.b", Value: new(any(3.14))}},
			},
			wantValues: []types.SyncParameterValueInput{{Path: "a.b", Value: new(any("3.14"))}},
		},
		{
			name: "string",
			input: types.SyncParameterValuesInput{
				DataSourceId: "ds-1",
				Values:       []types.SyncParameterValueInput{{Path: "a.b", Value: new(any("hello"))}},
			},
			wantValues: []types.SyncParameterValueInput{{Path: "a.b", Value: new(any(`"hello"`))}},
		},
		{
			name: "nil value removes parameter",
			input: types.SyncParameterValuesInput{
				DataSourceId: "ds-1",
				Values:       []types.SyncParameterValueInput{{Path: "a.b", Value: nil}},
			},
			wantValues: []types.SyncParameterValueInput{{Path: "a.b", Value: nil}},
		},
		{
			name: "map",
			input: types.SyncParameterValuesInput{
				DataSourceId: "ds-1",
				Values:       []types.SyncParameterValueInput{{Path: "a.b", Value: new(any(map[string]any{"key": "val"}))}},
			},
			wantValues: []types.SyncParameterValueInput{{Path: "a.b", Value: new(any(`{"key":"val"}`))}},
		},
		{
			name: "preserves DataSourceId",
			input: types.SyncParameterValuesInput{
				DataSourceId: "my-ds-id",
				Values:       []types.SyncParameterValueInput{{Path: "x", Value: new(any(1))}},
			},
			wantValues: []types.SyncParameterValueInput{{Path: "x", Value: new(any("1"))}},
		},
		{
			name: "multiple values mixed types",
			input: types.SyncParameterValuesInput{
				DataSourceId: "ds-1",
				Values: []types.SyncParameterValueInput{
					{Path: "a", Value: new(any(true))},
					{Path: "b", Value: nil},
					{Path: "c", Value: new(any(99))},
				},
			},
			wantValues: []types.SyncParameterValueInput{
				{Path: "a", Value: new(any("true"))},
				{Path: "b", Value: nil},
				{Path: "c", Value: new(any("99"))},
			},
		},
		{
			name: "empty values list",
			input: types.SyncParameterValuesInput{
				DataSourceId: "ds-1",
				Values:       []types.SyncParameterValueInput{},
			},
			wantValues: []types.SyncParameterValueInput{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := services.MarshalSyncParameterValues(tc.input)

			if tc.wantErrSubstr != "" {
				require.ErrorContains(t, err, tc.wantErrSubstr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.input.DataSourceId, got.DataSourceId)
			require.Len(t, got.Values, len(tc.wantValues))

			for i, want := range tc.wantValues {
				assert.Equal(t, want.Path, got.Values[i].Path)

				if want.Value == nil {
					assert.Nil(t, got.Values[i].Value)
				} else {
					require.NotNil(t, got.Values[i].Value)
					assert.Equal(t, *want.Value, *got.Values[i].Value)
				}
			}
		})
	}
}
