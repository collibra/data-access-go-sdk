package utils

import (
	"os"

	"github.com/stretchr/testify/suite"
)

type EnvConfig struct {
	User     string
	Password string
	URL      string
}

func getEnv(suite *suite.Suite, key string) string {
	value := os.Getenv(key)

	if value == "" {
		suite.FailNowf("Environment variable %s must be set for e2e tests", key)
	}

	return value
}

func GetEnvConfig(suite *suite.Suite) EnvConfig {
	return EnvConfig{
		User:     getEnv(suite, "COLLIBRA_USER"),
		Password: getEnv(suite, "COLLIBRA_PASSWORD"),
		URL:      getEnv(suite, "COLLIBRA_URL"),
	}
}