package agentharness_test

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const repositoryRoot = "../.."

var (
	backtickedToken     = regexp.MustCompile("`([^`\n]+)`")
	makeTarget          = regexp.MustCompile(`^make ([\w-]+)`)
	goTestRunPattern    = regexp.MustCompile(`-run (\S+)`)
	fragmentSpread      = regexp.MustCompile(`^\.{3}(\w+)$`)
	mixedCaseIdentifier = regexp.MustCompile(`^[A-Za-z_]*[a-z][A-Za-z_]*[A-Z]\w*$|^[A-Z][A-Za-z]*[a-z]\w*$`)
	environmentVariable = regexp.MustCompile(`^[A-Z][A-Z0-9]*(_[A-Z0-9]+)+$`)
	fileName            = regexp.MustCompile(`^[\w.-]+\.(go|graphql|yml|yaml|sh)$`)
)

func TestClaudeMdOnlyReferencesThingsThatExist(t *testing.T) {
	for _, token := range referencedTokens(t, readRepositoryFile(t, "CLAUDE.md")) {
		t.Run(token, func(t *testing.T) {
			assertReferenceExists(t, token)
		})
	}
}

func referencedTokens(t *testing.T, markdown string) []string {
	t.Helper()

	var tokens []string
	seen := map[string]bool{}

	for _, match := range backtickedToken.FindAllStringSubmatch(markdown, -1) {
		if !seen[match[1]] {
			seen[match[1]] = true
			tokens = append(tokens, match[1])
		}
	}

	require.NotEmpty(t, tokens, "CLAUDE.md names nothing in backticks")

	return tokens
}

func assertReferenceExists(t *testing.T, token string) {
	t.Helper()

	switch {
	case strings.Contains(token, "<"):
		t.Skip("placeholder")
	case makeTarget.MatchString(token):
		target := makeTarget.FindStringSubmatch(token)[1]
		require.Regexp(t, `(?m)^`+regexp.QuoteMeta(target)+`:`, readRepositoryFile(t, "Makefile"))
	case strings.HasPrefix(token, "go test "):
		assertGoTestArgumentsExist(t, token)
	case fragmentSpread.MatchString(token):
		assertSomeFileMentions(t, "fragment "+fragmentSpread.FindStringSubmatch(token)[1]+" on", ".graphql")
	case environmentVariable.MatchString(token):
		assertSomeFileMentions(t, `\b`+token+`\b`, "")
	case strings.Contains(token, "/") && !strings.Contains(token, " "):
		assertPathExists(t, token)
	case fileName.MatchString(token):
		assertSomeFileHasName(t, token)
	default:
		assertIdentifierExists(t, token)
	}
}

func assertGoTestArgumentsExist(t *testing.T, command string) {
	t.Helper()

	for argument := range strings.FieldsSeq(command) {
		if strings.HasPrefix(argument, ".") {
			assertPathExists(t, strings.TrimSuffix(argument, "..."))
		}
	}

	runPattern := goTestRunPattern.FindStringSubmatch(command)
	if runPattern == nil {
		return
	}

	isTestNameSeparator := func(character rune) bool { return strings.ContainsRune("/|", character) }
	for _, testName := range strings.FieldsFunc(strings.Trim(runPattern[1], "'"), isTestNameSeparator) {
		assertSomeFileMentions(t, `func (\([^)]*\) )?`+regexp.QuoteMeta(testName)+`\w*\(`, "_test.go")
	}
}

func assertPathExists(t *testing.T, path string) {
	t.Helper()

	if isGitIgnored(t, path) {
		t.Skip("gitignored, absent on a fresh clone")
	}

	_, err := os.Stat(filepath.Join(repositoryRoot, path))
	require.NoError(t, err, "%s does not exist", path)
}

func isGitIgnored(t *testing.T, path string) bool {
	t.Helper()

	command := exec.CommandContext(t.Context(), "git", "check-ignore", "--quiet", path)
	command.Dir = repositoryRoot

	err := command.Run()
	if err == nil {
		return true
	}

	var exitError *exec.ExitError
	require.ErrorAs(t, err, &exitError)
	require.Equal(t, 1, exitError.ExitCode(), "git check-ignore failed for %s", path)

	return false
}

func assertIdentifierExists(t *testing.T, token string) {
	t.Helper()

	identifier := token
	if open := strings.Index(identifier, "("); open >= 0 {
		identifier = identifier[:open]
	}

	if dot := strings.LastIndex(identifier, "."); dot >= 0 {
		identifier = identifier[dot+1:]
	}

	if !mixedCaseIdentifier.MatchString(identifier) {
		t.Skip("not an identifier")
	}

	assertSomeFileMentions(t, `\b`+regexp.QuoteMeta(identifier)+`\b`, ".go")
}

func assertSomeFileHasName(t *testing.T, name string) {
	t.Helper()

	found := false

	walkRepositoryFiles(t, func(path string) bool {
		found = filepath.Base(path) == name

		return !found
	})
	require.True(t, found, "no file named %s", name)
}

func assertSomeFileMentions(t *testing.T, pattern string, fileSuffix string) {
	t.Helper()

	expression := regexp.MustCompile(pattern)
	found := false

	walkRepositoryFiles(t, func(path string) bool {
		if !strings.HasSuffix(path, fileSuffix) {
			return true
		}

		content, err := os.ReadFile(path)
		require.NoError(t, err)

		found = expression.Match(content)

		return !found
	})
	require.True(t, found, "nothing matches %q", pattern)
}

func walkRepositoryFiles(t *testing.T, visit func(path string) (shouldContinue bool)) {
	t.Helper()

	err := filepath.WalkDir(repositoryRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == ".idea") {
			return filepath.SkipDir
		}

		if entry.IsDir() || entry.Name() == "generated.go" || strings.HasSuffix(entry.Name(), ".md") {
			return nil
		}

		if !visit(path) {
			return filepath.SkipAll
		}

		return nil
	})
	require.NoError(t, err)
}

func readRepositoryFile(t *testing.T, name string) string {
	t.Helper()

	content, err := os.ReadFile(filepath.Join(repositoryRoot, name))
	require.NoError(t, err)

	return string(content)
}
