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

func getEnv(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}

func TestMain(m *testing.M) {
    baseURL  := getEnv("COLLIBRA_BASE_URL", "https://access-governance-e2e-1.collibra.tech/accessGovernance")
    userName := getEnv("COLLIBRA_USERNAME", "Admin")
    password := getEnv("COLLIBRA_PASSWORD", "admin")

    client = collibra.NewClient(userName, password, baseURL)
    if client == nil {
        panic("Failed to create Collibra client")
    }
    ctx = context.Background()

    os.Exit(m.Run())
}
