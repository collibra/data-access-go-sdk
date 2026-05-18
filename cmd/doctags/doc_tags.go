// doctags is a code-generation helper that enriches a genqlient-generated Go
// file with `doc:"..."` struct tags sourced from the GraphQL schema's field
// descriptions. It parses the schema and the generated Go file, matches struct
// fields to their GraphQL counterparts (case-insensitively), and writes the
// updated AST back to disk.
//
// Usage:
//
//	go run doctags/doc_tags.go -schema schema.graphql -generated generated.go -output out.go
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/vektah/gqlparser/v2"
	gql_ast "github.com/vektah/gqlparser/v2/ast" // Aliased for clarity
)

func main() {
	// Parse command line flags
	inputFile := flag.String("schema", "", "Input GraphQL schema file path")
	generatedFile := flag.String("generated", "", "Input Go generated file path")
	outputFile := flag.String("output", "", "Output Go file path")

	flag.Parse()

	// Validate input parameters
	if *inputFile == "" || *outputFile == "" {
		slog.Error("Usage: go run main.go -schema schema.graphql -generated generated.go -output <output_file_path>")
		os.Exit(1)
	}

	// 1. Load and Parse your GraphQL Schema
	schemaSource, err := os.ReadFile(*inputFile)
	if err != nil {
		log.Fatalf("failed to read schema: %v", err)
	}

	schema, gerr := gqlparser.LoadSchema(&gql_ast.Source{Name: *inputFile, Input: string(schemaSource)})
	if gerr != nil {
		log.Fatalf("failed to parse schema: %v", gerr)
	}

	// 2. Parse the generated Go file
	fset := token.NewFileSet()

	node, err := parser.ParseFile(fset, *generatedFile, nil, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}

	// 3. Traverse the Go AST
	ast.Inspect(node, func(n ast.Node) bool {
		// Look for Struct Type Definitions
		typeSpec, ok := n.(*ast.TypeSpec)
		if !ok {
			return true
		}

		structType, ok := typeSpec.Type.(*ast.StructType)
		if !ok {
			return true
		}

		// Find the corresponding GraphQL type in the schema
		gqlType, exists := schema.Types[typeSpec.Name.Name]
		if !exists {
			return true
		}

		for _, field := range structType.Fields.List {
			if len(field.Names) == 0 {
				continue
			}

			goFieldName := field.Names[0].Name

			// Match Go field to GraphQL field description
			// We lowercase/match based on how genqlient maps names
			for _, gqlField := range gqlType.Fields {
				if strings.EqualFold(gqlField.Name, goFieldName) && gqlField.Description != "" {
					injectDocTag(field, gqlField.Description)
				}
			}
		}

		return true
	})

	// 4. Write back to file
	f, _ := os.Create(*outputFile)

	defer func() { _ = f.Close() }()

	err = format.Node(f, fset, node)
	if err != nil {
		log.Fatal(err) //nolint:gocritic
	}

	slog.Info("Doc tags completed successfully.")
}

func injectDocTag(field *ast.Field, description string) {
	// Clean up description (remove newlines/quotes)
	description = strings.ReplaceAll(description, "\n", " ")
	description = strings.ReplaceAll(description, "\"", "\\\"")
	description = strings.ReplaceAll(description, "`", "'")

	tagValue := fmt.Sprintf("doc:%q", description)

	if field.Tag == nil {
		field.Tag = &ast.BasicLit{Kind: token.STRING, Value: "`" + tagValue + "`"}
	} else {
		current := strings.Trim(field.Tag.Value, "`")
		if !strings.Contains(current, "doc:") {
			field.Tag.Value = "`" + current + " " + tagValue + "`"
		}
	}
}
