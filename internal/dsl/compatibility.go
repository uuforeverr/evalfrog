package dsl

import "sort"

// CompatibilityChecker is constructed from the Runtime's installed operation
// handlers. CheckAll must run before the Engine starts any node in a snapshot.
type CompatibilityChecker struct {
	supported map[Coordinate]struct{}
}

func NewCompatibilityChecker(coordinates ...Coordinate) CompatibilityChecker {
	supported := make(map[Coordinate]struct{}, len(coordinates))
	for _, coordinate := range coordinates {
		supported[coordinate] = struct{}{}
	}
	return CompatibilityChecker{supported: supported}
}

func BuiltinV1Compatibility() CompatibilityChecker {
	return NewCompatibilityChecker(
		Coordinate{Type: "control.start", Version: 1},
		Coordinate{Type: "control.end", Version: 1},
		Coordinate{Type: "control.branch", Version: 1},
		Coordinate{Type: "task.python", Version: 1},
		Coordinate{Type: "task.http", Version: 1},
		Coordinate{Type: "task.rpc", Version: 1},
	)
}

func BuiltinV2Compatibility() CompatibilityChecker {
	return NewCompatibilityChecker(
		Coordinate{Type: "control.start", Version: 1},
		Coordinate{Type: "control.end", Version: 1},
		Coordinate{Type: "control.start", Version: 2},
		Coordinate{Type: "control.end", Version: 2},
		Coordinate{Type: "control.branch", Version: 1},
		Coordinate{Type: "control.branch", Version: 2},
		Coordinate{Type: "task.python", Version: 1},
		Coordinate{Type: "task.http", Version: 1},
		Coordinate{Type: "task.rpc", Version: 1},
	)
}

func (checker CompatibilityChecker) CheckAll(document Document) []Issue {
	issues := make([]Issue, 0)
	for _, node := range document.Nodes {
		if _, exists := checker.supported[node.Operation.Coordinate()]; exists {
			continue
		}
		issues = append(issues, Issue{
			Code:    "RUNTIME_OPERATION_UNSUPPORTED",
			Message: "runtime does not support the operation coordinate",
			NodeID:  node.ID,
			Field:   "operation.version",
		})
	}
	sortIssues(issues)
	return issues
}

func (checker CompatibilityChecker) Coordinates() []Coordinate {
	result := make([]Coordinate, 0, len(checker.supported))
	for coordinate := range checker.supported {
		result = append(result, coordinate)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Type != result[right].Type {
			return result[left].Type < result[right].Type
		}
		return result[left].Version < result[right].Version
	})
	return result
}
