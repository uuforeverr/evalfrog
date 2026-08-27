package scheduling

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/uu999/evalfrog/internal/dsl"
)

// ResourceClass selects the Kafka task topic and the compatible Worker pool.
// It is routing metadata, not a Scheduler admission concept.
type ResourceClass string

const (
	ResourceBuiltin ResourceClass = "builtin"
	ResourceSandbox ResourceClass = "sandbox"
)

func (value ResourceClass) Valid() bool {
	return value == ResourceBuiltin || value == ResourceSandbox
}

type Router interface {
	Resolve(dsl.Coordinate) (ResourceClass, bool)
}

type StaticRouter map[dsl.Coordinate]ResourceClass

func (router StaticRouter) Resolve(coordinate dsl.Coordinate) (ResourceClass, bool) {
	value, exists := router[coordinate]
	return value, exists
}

func BuiltinV1Router() StaticRouter {
	return StaticRouter{
		{Type: "task.python", Version: 1}: ResourceSandbox,
		{Type: "task.http", Version: 1}:   ResourceBuiltin,
		{Type: "task.rpc", Version: 1}:    ResourceBuiltin,
	}
}

func RequiredCapabilities(class ResourceClass) []dsl.Coordinate {
	result := make([]dsl.Coordinate, 0, 2)
	for coordinate, routedClass := range BuiltinV1Router() {
		if routedClass == class {
			result = append(result, coordinate)
		}
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Type != result[right].Type {
			return result[left].Type < result[right].Type
		}
		return result[left].Version < result[right].Version
	})
	return result
}

func CapabilityFingerprint(class ResourceClass) string {
	digest := sha256.New()
	for _, coordinate := range RequiredCapabilities(class) {
		_, _ = fmt.Fprintf(digest, "%s@%d\n", coordinate.Type, coordinate.Version)
	}
	return hex.EncodeToString(digest.Sum(nil))
}
