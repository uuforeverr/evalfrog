package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/uu999/evalfrog/internal/dsl"
)

type branchCase struct {
	Route    string          `json:"route"`
	Path     string          `json:"path,omitempty"`
	Operator string          `json:"operator"`
	Value    json.RawMessage `json:"value"`
}

type branchCaseV2 struct {
	Route      string `json:"route"`
	Logic      string `json:"logic"`
	Conditions []struct {
		Input    string          `json:"input"`
		Path     string          `json:"path,omitempty"`
		Operator string          `json:"operator"`
		Value    json.RawMessage `json:"value"`
	} `json:"conditions"`
}

func evaluateBranchV2(node dsl.Node, inputs map[string]json.RawMessage) (string, *Error) {
	var cases []branchCaseV2
	var fallback string
	if json.Unmarshal(node.Operation.Config["cases"], &cases) != nil || json.Unmarshal(node.Operation.Config["default_route"], &fallback) != nil {
		return "", &Error{Code: FailureBranchOperatorMismatch, NodeID: string(node.ID), Field: "operation.config"}
	}
	for caseIndex, candidate := range cases {
		matched := candidate.Logic == "and"
		for conditionIndex, condition := range candidate.Conditions {
			field := fmt.Sprintf("operation.config.cases.%d.conditions.%d", caseIndex, conditionIndex)
			raw, exists := inputs[condition.Input]
			if !exists {
				return "", &Error{Code: FailureBranchPathNotFound, NodeID: string(node.ID), Field: field + ".input"}
			}
			actual, err := decodeJSON(raw)
			if err != nil {
				return "", &Error{Code: FailureBranchOperatorMismatch, NodeID: string(node.ID), Field: field + ".input", Cause: err}
			}
			if condition.Path != "" {
				actual, exists = lookupPath(actual, condition.Path)
				if !exists {
					return "", &Error{Code: FailureBranchPathNotFound, NodeID: string(node.ID), Field: field + ".path"}
				}
			}
			expected, err := decodeJSON(condition.Value)
			if err != nil {
				return "", &Error{Code: FailureBranchOperatorMismatch, NodeID: string(node.ID), Field: field + ".value", Cause: err}
			}
			result, err := compareBranch(actual, condition.Operator, expected)
			if err != nil {
				return "", &Error{Code: FailureBranchOperatorMismatch, NodeID: string(node.ID), Field: field, Cause: err}
			}
			if candidate.Logic == "and" {
				matched = matched && result
			} else {
				matched = matched || result
			}
		}
		if matched {
			return candidate.Route, nil
		}
	}
	return fallback, nil
}

func evaluateBranch(node dsl.Node, raw json.RawMessage) (string, *Error) {
	var cases []branchCase
	var fallback string
	if json.Unmarshal(node.Operation.Config["cases"], &cases) != nil || json.Unmarshal(node.Operation.Config["default_route"], &fallback) != nil {
		return "", &Error{Code: FailureBranchOperatorMismatch, NodeID: string(node.ID), Field: "operation.config"}
	}
	value, err := decodeJSON(raw)
	if err != nil {
		return "", &Error{Code: FailureBranchOperatorMismatch, NodeID: string(node.ID), Field: "inputs.value", Cause: err}
	}
	for index, candidate := range cases {
		actual := value
		if candidate.Path != "" {
			var exists bool
			actual, exists = lookupPath(value, candidate.Path)
			if !exists {
				return "", &Error{Code: FailureBranchPathNotFound, NodeID: string(node.ID), Field: fmt.Sprintf("operation.config.cases.%d.path", index)}
			}
		}
		expected, decodeErr := decodeJSON(candidate.Value)
		if decodeErr != nil {
			return "", &Error{Code: FailureBranchOperatorMismatch, NodeID: string(node.ID), Field: fmt.Sprintf("operation.config.cases.%d.value", index), Cause: decodeErr}
		}
		matched, compareErr := compareBranch(actual, candidate.Operator, expected)
		if compareErr != nil {
			return "", &Error{Code: FailureBranchOperatorMismatch, NodeID: string(node.ID), Field: fmt.Sprintf("operation.config.cases.%d", index), Cause: compareErr}
		}
		if matched {
			return candidate.Route, nil
		}
	}
	return fallback, nil
}

func decodeJSON(raw json.RawMessage) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func lookupPath(value any, path string) (any, bool) {
	current := value
	for _, segment := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[segment]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func compareBranch(actual any, operator string, expected any) (bool, error) {
	switch operator {
	case "eq", "neq":
		if !comparableKinds(actual, expected) {
			return false, fmt.Errorf("equality operands have incompatible JSON types")
		}
		equal, err := equalJSON(actual, expected)
		if err != nil {
			return false, err
		}
		if operator == "neq" {
			equal = !equal
		}
		return equal, nil
	case "gt", "gte", "lt", "lte":
		left, leftOK := rational(actual)
		right, rightOK := rational(expected)
		if !leftOK || !rightOK {
			return false, fmt.Errorf("numeric operator requires numbers")
		}
		comparison := left.Cmp(right)
		switch operator {
		case "gt":
			return comparison > 0, nil
		case "gte":
			return comparison >= 0, nil
		case "lt":
			return comparison < 0, nil
		default:
			return comparison <= 0, nil
		}
	case "contains", "not_contains":
		contains := false
		switch typed := actual.(type) {
		case string:
			candidate, ok := expected.(string)
			if !ok {
				return false, fmt.Errorf("string contains requires a string")
			}
			contains = strings.Contains(typed, candidate)
		case []any:
			for _, item := range typed {
				equal, err := equalJSON(item, expected)
				if err != nil {
					return false, err
				}
				if equal {
					contains = true
					break
				}
			}
		default:
			return false, fmt.Errorf("contains requires string or array")
		}
		if operator == "not_contains" {
			contains = !contains
		}
		return contains, nil
	case "starts_with", "ends_with":
		left, leftOK := actual.(string)
		right, rightOK := expected.(string)
		if !leftOK || !rightOK {
			return false, fmt.Errorf("string operator requires strings")
		}
		if operator == "starts_with" {
			return strings.HasPrefix(left, right), nil
		}
		return strings.HasSuffix(left, right), nil
	case "has_key", "not_has_key":
		object, objectOK := actual.(map[string]any)
		key, keyOK := expected.(string)
		if !objectOK || !keyOK {
			return false, fmt.Errorf("has_key requires object and string")
		}
		_, exists := object[key]
		if operator == "not_has_key" {
			exists = !exists
		}
		return exists, nil
	default:
		return false, fmt.Errorf("operator %q is unsupported", operator)
	}
}

func comparableKinds(left, right any) bool {
	_, leftNumber := rational(left)
	_, rightNumber := rational(right)
	if leftNumber || rightNumber {
		return leftNumber && rightNumber
	}
	switch left.(type) {
	case nil:
		return right == nil
	case string:
		_, ok := right.(string)
		return ok
	case bool:
		_, ok := right.(bool)
		return ok
	case []any:
		_, ok := right.([]any)
		return ok
	case map[string]any:
		_, ok := right.(map[string]any)
		return ok
	default:
		return false
	}
}

func equalJSON(left, right any) (bool, error) {
	leftNumber, leftIsNumber := rational(left)
	rightNumber, rightIsNumber := rational(right)
	if leftIsNumber || rightIsNumber {
		if !leftIsNumber || !rightIsNumber {
			return false, nil
		}
		return leftNumber.Cmp(rightNumber) == 0, nil
	}
	switch typed := left.(type) {
	case nil:
		return right == nil, nil
	case string:
		candidate, ok := right.(string)
		return ok && typed == candidate, nil
	case bool:
		candidate, ok := right.(bool)
		return ok && typed == candidate, nil
	case []any:
		candidate, ok := right.([]any)
		if !ok || len(typed) != len(candidate) {
			return false, nil
		}
		for index := range typed {
			equal, err := equalJSON(typed[index], candidate[index])
			if err != nil || !equal {
				return false, err
			}
		}
		return true, nil
	case map[string]any:
		candidate, ok := right.(map[string]any)
		if !ok || len(typed) != len(candidate) {
			return false, nil
		}
		for key, value := range typed {
			other, exists := candidate[key]
			if !exists {
				return false, nil
			}
			equal, err := equalJSON(value, other)
			if err != nil || !equal {
				return false, err
			}
		}
		return true, nil
	default:
		return false, fmt.Errorf("unsupported JSON value type")
	}
}

func rational(value any) (*big.Rat, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return nil, false
	}
	result, ok := new(big.Rat).SetString(number.String())
	return result, ok
}
