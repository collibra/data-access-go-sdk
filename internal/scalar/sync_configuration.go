package scalar

import (
	"gopkg.in/yaml.v3"
)

func MarshalSyncConfiguration(v *map[string]any) ([]byte, error) { //nolint:gocritic
	if v == nil {
		return nil, nil
	}

	return yaml.Marshal(v) //nolint:wrapcheck
}

func UnmarshalSyncConfiguration(b []byte, v *map[string]any) error { //nolint:gocritic
	if len(b) == 0 {
		return nil
	}

	return yaml.Unmarshal(b, v) //nolint:wrapcheck
}
