# Access Governance Go SDK


**Note: This repository is still in an early stage of development.
At this point, no contributions are accepted to the project yet.**

This repository contains a simple SDK for Collibra Access Governance.
It can be used to automate basic operations.

## Installation
```shell
go get -u github.com/collibra/access-governance-go-sdk
```

## Examples
```go
package main

import (
    "context"
    "fmt"

	collibra "github.com/collibra/access-governance-go-sdk"
)

func main() {
	ctx := context.Background()
	
	// Create a new Collibra Access Governance Client 
	client := collibra.NewClient(ctx, "your-user", "your-password")
	
	// Access the AccessControlClient 
	accessControlClient := client.AccessControl()
	ac, err := accessControlClient.GetAccessControl(ctx, "ap-id")
	if(err != nil) {
		panic("ap does not exist: " + err.Error())
	}
	fmt.Printf("AccessControl: %+v\n", ac)
}
```