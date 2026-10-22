package services_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/collibra/data-access-go-sdk/types"
)

func TestRootParameterDefinitionInput_omitsOauthWhenUnset(t *testing.T) {
	definition := types.RootParameterDefinitionInput{
		Connection: []types.ParameterDefinitionInput{{Key: "host", DisplayName: "Host", Type: types.ParameterTypeParameter}},
	}

	encoded, err := json.Marshal(definition)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Len(t, decoded["connection"], 1)
	assert.NotContains(t, decoded, "oauth")
}

func TestRootParameterDefinitionInput_marshalsOauthDefinition(t *testing.T) {
	definition := types.RootParameterDefinitionInput{
		Oauth: &types.OAuthDefinitionInput{ //nolint:gosec // G101 trips on the token field names; these are URL fixtures, not credentials.
			AuthorizationUrl:        "https://login.example.com/{tenant}/authorize",
			TokenUrl:                "https://login.example.com/{tenant}/token",
			Scopes:                  []string{"offline_access"},
			AuthorizationParameters: []types.OAuthAuthorizationParameterInput{{Key: "prompt", Value: "select_account"}},
			ClientParameters:        []types.ParameterDefinitionInput{{Key: "tenant", Type: types.ParameterTypeParameter}},
			AccessTokenParameter:    "access-token",
		},
	}

	encoded, err := json.Marshal(definition)
	require.NoError(t, err)

	var decoded struct {
		Oauth map[string]any `json:"oauth"`
	}
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, "https://login.example.com/{tenant}/authorize", decoded.Oauth["authorizationUrl"])
	assert.Equal(t, "https://login.example.com/{tenant}/token", decoded.Oauth["tokenUrl"])
	assert.Equal(t, []any{"offline_access"}, decoded.Oauth["scopes"])
	assert.Equal(t, []any{map[string]any{"key": "prompt", "value": "select_account"}}, decoded.Oauth["authorizationParameters"])
	assert.Equal(t, "access-token", decoded.Oauth["accessTokenParameter"])
	assert.Len(t, decoded.Oauth["clientParameters"], 1)
}
