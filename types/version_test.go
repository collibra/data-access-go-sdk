package types

import (
	"testing"

	"github.com/Masterminds/semver/v3"
	"github.com/stretchr/testify/assert"
)

func TestMarshalVersion(t *testing.T) {
	tests := []struct {
		name    string
		input   *Version
		want    []byte
		wantErr bool
	}{
		{
			name:  "nil version",
			input: nil,
			want:  nil,
		},
		{
			name:  "valid version",
			input: semver.MustParse("1.2.3"),
			want:  []byte(`"1.2.3"`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MarshalVersion(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshalVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUnmarshalVersion(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    Version
		wantErr bool
	}{
		{
			name:  "nil bytes",
			input: nil,
			want:  Version{},
		},
		{
			name:  "empty bytes",
			input: []byte{},
			want:  Version{},
		},
		{
			name:  "valid version",
			input: []byte(`"1.2.3"`),
			want:  *semver.MustParse("1.2.3"),
		},
		{
			name:    "invalid version string",
			input:   []byte(`"invalid"`),
			wantErr: true,
		},
		{
			name:    "invalid input format",
			input:   []byte(`1.2.3`),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v Version

			err := UnmarshalVersion(tt.input, &v)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				assert.Equal(t, tt.want, v)
			}
		})
	}
}
