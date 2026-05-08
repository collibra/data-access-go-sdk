# Collibra Data Access Go SDK

**Note: This repository is still in an early stage of development and should not be used in production.
Breaking changes can be made to this at any point in time.**

This repository contains a simple SDK for Collibra Data Access.
It can be used to automate basic operations.

## Installation
```shell
go get -u github.com/collibra/data-access-go-sdk
```

## Examples

```go
package main

import (
	"context"
	"fmt"

	collibra "github.com/collibra/data-access-go-sdk"
)

func main() {
	ctx := context.Background()

	// Create a new Collibra Data Access Client
	url := "https://your-deployment.collibra.com/dataAccess"
	client, err := collibra.NewClient(url, collibra.WithUsername("your-user"), collibra.WithPassword("your-password"))
	if err != nil {
		panic("can not create collibra client: " + err.Error())
	}

	// Access the AccessControlClient 
	accessControlClient := client.AccessControl()
	ac, err := accessControlClient.GetAccessControl(ctx, "ap-id")
	if err != nil {
		panic("ap does not exist: " + err.Error())
	}
	fmt.Printf("AccessControl: %+v\n", ac)
}
```