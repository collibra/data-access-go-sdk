package scalar

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestMarshalSyncConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		input   *map[string]any
		want    []byte
		wantErr bool
	}{
		{
			name:  "nil map",
			input: nil,
			want:  nil,
		},
		{
			name: "valid map",
			input: &map[string]any{
				"key": "value",
				"nested": map[string]any{
					"another": 123,
				},
			},
			want: func() []byte {
				b, _ := yaml.Marshal(&map[string]any{
					"key": "value",
					"nested": map[string]any{
						"another": 123,
					},
				})

				return b
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MarshalSyncConfiguration(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshalSyncConfiguration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUnmarshalSyncConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    map[string]any
		wantErr bool
	}{
		{
			name:  "nil bytes",
			input: nil,
			want:  nil,
		},
		{
			name:  "empty bytes",
			input: []byte{},
			want:  nil,
		},
		{
			name: "valid yaml",
			input: func() []byte {
				b, _ := yaml.Marshal(&map[string]any{
					"key": "value",
					"nested": map[string]any{
						"another": 123,
					},
				})

				return b
			}(),
			want: map[string]any{
				"key": "value",
				"nested": map[string]any{
					"another": 123,
				},
			},
		},
		{
			name:    "invalid yaml",
			input:   []byte(`:`),
			wantErr: true,
		},
		{
			name:  "invalid yaml",
			input: []byte(`"access-to-target-sync: {}\nconnection: {}\ndata-object-sync: {}\nglobal: {}\nidentities-sync: {}\nusage-sync: {}\n"`),
			want: map[string]any{
				"access-to-target-sync": map[string]any{},
				"connection":            map[string]any{},
				"data-object-sync":      map[string]any{},
				"global":                map[string]any{},
				"identities-sync":       map[string]any{},
				"usage-sync":            map[string]any{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v map[string]any

			err := UnmarshalSyncConfiguration(tt.input, &v)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalSyncConfiguration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				assert.Equal(t, tt.want, v)
			}
		})
	}
}

func TestUnmarshalGraphQLString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
		{
			name:  "regular string",
			input: "a regular string",
			want:  "a regular string",
		},
		{
			name:  "GraphQL quoted string with escapes",
			input: `"a regular string with \n new line and \t tab and \r escape and \" quote"`,
			want:  "a regular string with \n new line and \t tab and \r escape and \" quote",
		},
		{
			name:  "GraphQL quoted string with unicode escape",
			input: `"string with unicode \u00A9 symbol"`,
			want:  "string with unicode © symbol",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := UnmarshalGraphQLString(tt.input)
			assert.Equal(t, tt.want, v)
		})
	}
}
