package scalar

import (
	"strings"

	"gopkg.in/yaml.v3"
)

func MarshalSyncConfiguration(v *map[string]any) ([]byte, error) { //nolint:gocritic
	if v == nil {
		return nil, nil
	}

	return yaml.Marshal(v) //nolint:wrapcheck
}

func UnmarshalSyncConfiguration(b []byte, v *map[string]any) error { //nolint:gocritic
	if len(b) == 0 {
		return nil
	}

	s := UnmarshalGraphQLString(string(b))

	return yaml.Unmarshal([]byte(s), v) //nolint:wrapcheck
}

func UnmarshalGraphQLString(s string) string {
	if s == "" {
		return ""
	}

	if s[0] == '"' && s[len(s)-1] == '"' {
		// Trim the surrounding quotes
		s = s[1 : len(s)-1]
	}

	s = unescape(s)

	return s
}

func unescape(input string) string { //nolint:cyclop
	var builder strings.Builder
	builder.Grow(len(input))

	for i := 0; i < len(input); i++ {
		if input[i] == '\\' {
			if i+1 < len(input) {
				switch input[i+1] {
				case 't':
					builder.WriteByte('\t')

					i++
					continue
				case 'n':
					builder.WriteByte('\n')

					i++
					continue
				case 'r':
					builder.WriteByte('\r')

					i++
					continue
				case '\\':
					builder.WriteByte('\\')

					i++
					continue
				case '"':
					builder.WriteByte('"')

					i++
					continue
				}
			}
		}
		// Check for the beginning of a \uXXXX sequence
		if input[i] == '\\' && i+5 < len(input) && input[i+1] == 'u' {
			// Extract the 4 hex digits after \u
			hexPart := input[i+2 : i+6]

			// Convert hex string to integer manually
			var val rune
			valid := true

			for _, char := range hexPart {
				val <<= 4 // Shift left by 4 bits (multiply by 16)

				switch {
				case char >= '0' && char <= '9':
					val |= char - '0'
				case char >= 'a' && char <= 'f':
					val |= char - 'a' + 10
				case char >= 'A' && char <= 'F':
					val |= char - 'A' + 10
				default:
					valid = false
				}
			}

			if valid {
				builder.WriteRune(val) // Convert the code point to a UTF-8 character

				i += 5 // Skip the processed \uXXXX sequence
				continue
			}
		}
		// If not a sequence or invalid, write the character as-is
		builder.WriteByte(input[i])
	}

	return builder.String()
}
