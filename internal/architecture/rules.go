package architecture

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const ModulePath = "github.com/uu999/evalfrog"

type Graph map[string][]string

type Violation struct {
	From   string
	To     string
	Reason string
}

func (violation Violation) Error() string {
	if violation.To == "" {
		return fmt.Sprintf("%s: %s", violation.From, violation.Reason)
	}
	return fmt.Sprintf("%s imports %s: %s", violation.From, violation.To, violation.Reason)
}

var domainPrefixes = []string{
	"internal/access", "internal/resources", "internal/definition", "internal/ir", "internal/catalog",
	"internal/compiler", "internal/dsl", "internal/sourcemap", "internal/runtime", "internal/scheduling", "internal/eventing", "internal/recovery", "internal/projection",
	"internal/sandbox",
}

func Validate(graph Graph) []Violation {
	var violations []Violation
	for from, imports := range graph {
		fromRelative := localPath(from)
		for _, imported := range imports {
			toRelative := localPath(imported)
			if fromRelative == "internal/compiler" && hasAnyPrefix(imported, []string{
				"net/http", "github.com/jackc/pgx", "github.com/redis/go-redis", "github.com/twmb/franz-go",
			}) {
				violations = append(violations, Violation{from, imported, "compiler must remain deterministic and cannot import HTTP, PostgreSQL, Redis, or Kafka clients"})
			}
			if toRelative == "" {
				continue
			}
			if hasAnyPrefix(fromRelative, domainPrefixes) && strings.HasPrefix(toRelative, "internal/adapters/") {
				violations = append(violations, Violation{from, imported, "domain modules must depend on ports, not adapters"})
			}
			if strings.HasPrefix(fromRelative, "internal/worker/") && strings.HasPrefix(toRelative, "internal/adapters/postgres") {
				violations = append(violations, Violation{from, imported, "workers must not have PostgreSQL access"})
			}
			if strings.HasPrefix(fromRelative, "internal/worker/") && hasAnyPrefix(imported, []string{"github.com/jackc/pgx", "github.com/redis/go-redis"}) {
				violations = append(violations, Violation{from, imported, "worker processes must not carry PostgreSQL or Redis clients"})
			}
			if strings.HasPrefix(fromRelative, "internal/adapters/httpapi") && strings.HasPrefix(toRelative, "internal/adapters/postgres") {
				violations = append(violations, Violation{from, imported, "HTTP API handlers must call application ports instead of PostgreSQL repositories"})
			}
			if hasAnyPrefix(fromRelative, []string{"internal/runtime"}) && hasAnyPrefix(toRelative, []string{"internal/ir", "internal/definition", "internal/catalog"}) {
				violations = append(violations, Violation{from, imported, "runtime executes immutable DSL and must not read authoring models"})
			}
		}
	}
	sort.Slice(violations, func(left, right int) bool { return violations[left].Error() < violations[right].Error() })
	return violations
}

func LoadGraph(root string) (Graph, error) {
	graph := make(Graph)
	for _, top := range []string{"cmd", "internal"} {
		base := filepath.Join(root, top)
		err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				return nil
			}
			parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
			if err != nil {
				return fmt.Errorf("parse %s: %w", path, err)
			}
			directory := filepath.Dir(path)
			relative, err := filepath.Rel(root, directory)
			if err != nil {
				return err
			}
			from := ModulePath + "/" + filepath.ToSlash(relative)
			for _, spec := range parsed.Imports {
				value, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					return err
				}
				graph[from] = append(graph[from], value)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return graph, nil
}

func ValidateLayout(root string) []Violation {
	var violations []Violation
	for _, forbidden := range []string{"pkg", "common", "utils", "service"} {
		_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil || !entry.IsDir() || path == root {
				return nil
			}
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			if entry.Name() == forbidden {
				relative, _ := filepath.Rel(root, path)
				violations = append(violations, Violation{From: filepath.ToSlash(relative), Reason: "forbidden catch-all package name"})
				return filepath.SkipDir
			}
			return nil
		})
	}
	allowedCommands := map[string]bool{"evalfrog": true, "control-plane": true, "worker-builtin": true, "worker-sandbox": true}
	rootFS := os.DirFS(root)
	entries, err := fs.ReadDir(rootFS, "cmd")
	if err != nil {
		return append(violations, Violation{From: "cmd", Reason: err.Error()})
	}
	for _, entry := range entries {
		if entry.IsDir() && !allowedCommands[entry.Name()] {
			violations = append(violations, Violation{From: "cmd/" + entry.Name(), Reason: "unexpected deployable command; M0 freezes exactly four executables"})
		}
	}
	for command := range allowedCommands {
		if _, err := fs.Stat(rootFS, "cmd/"+command); err != nil {
			violations = append(violations, Violation{From: "cmd/" + command, Reason: "required executable is missing"})
		}
	}
	sort.Slice(violations, func(left, right int) bool { return violations[left].Error() < violations[right].Error() })
	return violations
}

func localPath(value string) string {
	return strings.TrimPrefix(strings.TrimPrefix(value, ModulePath), "/")
}

func hasAnyPrefix(value string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if value == prefix || strings.HasPrefix(value, prefix+"/") {
			return true
		}
	}
	return false
}
