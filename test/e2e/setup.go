package e2e_test

import (
	"context"
	"os"
	"testing"

	collibra "github.com/collibra/data-access-go-sdk"
)

var (
	client *collibra.CollibraClient

	ctx    context.Context
)

func getEnv(key string) string {
	value := os.Getenv(key)


    if value != "" {
        panic("Environment variable " + key + " must be set for e2e tests")
    }
    return value 
}

func TestMain(m *testing.M) {
	baseURL := getEnv("COLLIBRA_URL")
	userName := getEnv("COLLIBRA_USER")
	password := getEnv("COLLIBRA_PASSWORD")

	client = collibra.NewClient(userName, password, baseURL)
	if client == nil {
		panic("Failed to create Collibra client")
	}
	ctx = context.Background()

    if ctx == nil {
        panic("Failed to create context")
    }

	os.Exit(m.Run())
}
