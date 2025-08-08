<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/raito-io/raito-io.github.io/raw/master/assets/images/logo-vertical-dark%402x.png">
    <img height="250px" src="https://github.com/raito-io/raito-io.github.io/raw/master/assets/images/logo-vertical%402x.png">
  </picture>
</h1>

<h4 align="center">
  Raito SDK
</h4>

<p align="center">
    <a href="/LICENSE" target="_blank"><img src="https://img.shields.io/badge/license-Apache%202-brightgreen.svg" alt="Software License" /></a>
</p>

<hr/>

# Raito SDK


**Note: This repository is still in an early stage of development.
At this point, no contributions are accepted to the project yet.**

This repository contains a simple SDK for Raito Cloud.
It can be used to automate basic operations.

## Installation
```shell
go get -u github.com/raito-io/sdk-go
```

## Examples
```go
package main

import (
    "context"
    "fmt"

    raito "github.com/raito-io/sdk-go"
)

func main() {
	ctx := context.Background()
	
	// Create a new RaitoClient 
	client := raito.NewClient(ctx, "your-domain", "your-user", "your-secret")
	
	// Access the AccessProviderClient 
	accessProviderClient := client.AccessProvider()
	ap, err := accessProviderClient.GetAccessProvider(ctx, "ap-id")
	if err != nil {
	    panic("ap does not exist: " + err.Error())
	}
	
	fmt.Printf("AccessProvider: %+v\n", ap)
}
```