package e2e_test

import (
	"context"
	"os"
	"testing"

	Collibra "github.com/collibra/data-access-go-sdk"
)

var (
	baseURL  = getEnv("COLLIBRA_URL")
	userName = getEnv("COLLIBRA_USER")
	password = getEnv("COLLIBRA_PASSWORD")

	client = Collibra.NewClient(userName, password, baseURL)

	ctx = context.Background()
)

func getEnv(key string) string {
	value := os.Getenv(key)

	if value == "" {
		panic("Environment variable " + key + " must be set for e2e tests")
	}

	return value
}

func TestE2ESetUp(t *testing.T) {
	if client == nil {
		t.Fatal("Failed to create Collibra client")
	}

	if ctx == nil {
		t.Fatal("Failed to create context")
	}
}
