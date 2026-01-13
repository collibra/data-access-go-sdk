package utils

import (
	"os"

	"github.com/collibra/data-access-go-sdk/internal"
	"github.com/stretchr/testify/suite"
)

type ClientOptions = func(*internal.ClientOptions)

type EnvConfig struct {
	User     string
	Password string
	URL      string
}

func getEnv(suite *suite.Suite, key string) string {
	value := os.Getenv(key)

	if value == "" {
		suite.FailNowf("Missing environment variable", "Environment variable %s must be set for e2e tests", key)
	}

	return value
}

func GetEnvConfig(suite *suite.Suite) (string, []ClientOptions) {
	url := getEnv(suite, "COLLIBRA_URL")
	options := []ClientOptions{
		func(ops *internal.ClientOptions) {
			ops.Username = getEnv(suite, "COLLIBRA_USER")
		},
		func(ops *internal.ClientOptions) {
			ops.Username = getEnv(suite, "COLLIBRA_PASSWORD")
		},
	}

	return url, options
}
