package types

import (
	"fmt"
	"strings"
)

const (
	TypeDelimiter       = ":"
	PathDelimiter       = "."
	PathEscapeCharacter = "\\"
)

type FullName struct {
	Type string   `json:"type,omitempty"`
	Path []string `json:"path,omitempty"`
}

// FromDataObjectURI parses a full name URI string into a FullName struct.
// The format of the fullName is type:path.part1.part2.part3 where path parts can contain any unicode character.
// The type and path are separated by a colon (:). The path parts are separated by a dot (.).
// The dot (.) and backslash (\) characters in path parts are escaped with a backslash (\).
// For example: "myType:part1.part2\..part3" would be parsed as type "myType" and path ["part1", "part2.", "part3"].
func FromDataObjectURI(fullName string) (*FullName, error) {
	if fullName == "" {
		return &FullName{
			Type: "",
			Path: []string{},
		}, nil
	}

	typeStr, pathStr, ok := strings.Cut(fullName, TypeDelimiter)
	if !ok {
		return nil, fmt.Errorf("no type delimiter found for full name: %s", fullName)
	}

	var pathParts []string
	var currentPart strings.Builder
	escaped := false

	for _, char := range pathStr {
		charStr := string(char)

		switch escaped {
		case true:
			// If the previous character was '\', treat this character as literal data
			currentPart.WriteRune(char)

			escaped = false
		default:
			switch charStr {
			case PathEscapeCharacter:
				// Found the start of an escape sequence
				escaped = true
			case PathDelimiter:
				// Found an unescaped delimiter, meaning the current part is complete
				pathParts = append(pathParts, currentPart.String())
				currentPart.Reset()
			default:
				// Normal, unescaped character
				currentPart.WriteRune(char)
			}
		}
	}

	pathParts = append(pathParts, currentPart.String())

	return &FullName{
		Type: typeStr,
		Path: pathParts,
	}, nil
}

// ToDataObjectURI converts a FullName struct into a full name URI string.
func (f *FullName) ToDataObjectURI() string {
	buf := strings.Builder{}
	buf.WriteString(f.Type)
	buf.WriteString(TypeDelimiter)

	isFirstPart := true

	for _, part := range f.Path {
		// Step A: Add path delimiter (e.g., ".")
		if !isFirstPart {
			buf.WriteString(PathDelimiter)
		} else {
			isFirstPart = false
		}

		// Step B: Escape the Escape Character first (e.g., replace "\" with "\\")
		temp := strings.ReplaceAll(part, PathEscapeCharacter, PathEscapeCharacter+PathEscapeCharacter)

		// Step C: Escape the Delimiter (e.g., replace "." with "\.")
		buf.WriteString(strings.ReplaceAll(temp, PathDelimiter, PathEscapeCharacter+PathDelimiter))
	}

	return buf.String()
}
