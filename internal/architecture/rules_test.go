package architecture

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRepositoryArchitecture(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	graph, err := LoadGraph(root)
	if err != nil {
		t.Fatal(err)
	}
	violations := append(Validate(graph), ValidateLayout(root)...)
	if len(violations) != 0 {
		messages := make([]string, len(violations))
		for index, violation := range violations {
			messages[index] = violation.Error()
		}
		t.Fatalf("architecture violations:\n%s", strings.Join(messages, "\n"))
	}
}

func TestRulesRejectForbiddenImports(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		from   string
		to     string
		reason string
	}{
		{"domain adapter", "internal/definition", "internal/adapters/postgres", "domain modules"},
		{"worker database", "internal/worker/runtime", "internal/adapters/postgres", "workers must not"},
		{"worker pgx", "internal/worker/runtime", "github.com/jackc/pgx/v5", "must not carry"},
		{"worker redis", "internal/worker/runtime", "github.com/redis/go-redis/v9", "must not carry"},
		{"HTTP API database", "internal/adapters/httpapi", "internal/adapters/postgres", "application ports"},
		{"runtime authoring", "internal/runtime/engine", "internal/ir", "authoring models"},
		{"runtime root authoring", "internal/runtime", "internal/definition", "authoring models"},
		{"compiler kafka", "internal/compiler", "github.com/twmb/franz-go", "compiler must remain deterministic"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			imported := ModulePath + "/" + test.to
			if strings.HasPrefix(test.to, "github.com/") || strings.HasPrefix(test.to, "net/") {
				imported = test.to
			}
			graph := Graph{ModulePath + "/" + test.from: {imported}}
			violations := Validate(graph)
			if len(violations) == 0 || !strings.Contains(violations[0].Reason, test.reason) {
				t.Fatalf("expected %q violation, got %+v", test.reason, violations)
			}
		})
	}
}
