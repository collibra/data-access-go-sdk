package types

import (
	"fmt"

	"github.com/Masterminds/semver/v3"
)

type Version = semver.Version

func MarshalVersion(v *Version) ([]byte, error) {
	if v == nil {
		return nil, nil
	}

	versionStr := v.String()

	return fmt.Appendf(nil, "%q", versionStr), nil
}

func UnmarshalVersion(b []byte, v *Version) error {
	if len(b) == 0 {
		return nil
	}

	var versionStr string

	_, err := fmt.Sscanf(string(b), "%q", &versionStr)
	if err != nil {
		return fmt.Errorf("could not unmarshal version string: %w", err)
	}

	version, err := semver.NewVersion(versionStr)
	if err != nil {
		return fmt.Errorf("could not parse version string: %w", err)
	}

	*v = *version

	return nil
}
