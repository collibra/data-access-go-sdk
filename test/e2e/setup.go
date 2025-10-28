package e2e_test

import (
	"context"
	"os"

	collibra "github.com/collibra/data-access-go-sdk"
)

var (
    baseURL  = getEnv("COLLIBRA_BASE_URL", "https://access-governance-e2e-1.collibra.tech/accessGovernance")
    userName = getEnv("COLLIBRA_USERNAME", "Admin")
    password = getEnv("COLLIBRA_PASSWORD", "admin")
    
	client   = collibra.NewClient(userName, password, baseURL)
	ctx      = context.Background()
)

func getEnv(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}
