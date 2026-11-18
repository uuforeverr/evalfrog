//go:build integration

package integration

import (
	"errors"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/projection"
	runtimepkg "github.com/uu999/evalfrog/internal/runtime"
)

// Project scope is asserted at the actual MySQL projection boundary. The
// second principal has RunRead in its own Project, so this proves that a valid
// permission in B cannot turn an ID from A into a readable resource.
func TestM12RunAndDiagnosticsStayInsideProjectScope(t *testing.T) {
	harness := newM5Harness(t)
	workflow, snapshot := harness.createCodeWorkflow(t, false)
	run := harness.createTestRun(t, workflow.ID, snapshot.ID, "m12-project-isolation")
	otherProject, otherPrincipal, otherExecution := newID(t), newID(t), newID(t)
	harness.seedProject(t, otherProject, otherPrincipal, otherExecution, "m12-other-project-token-"+newID(t), allPermissions())
	reader := projection.NewBuiltinService(harness.store, harness.access)
	if _, err := reader.GetRun(harness.ctx, otherProject, otherPrincipal, run.ID); !errors.Is(err, runtimepkg.ErrRunNotFound) {
		t.Fatalf("cross-project run read error=%v, want not found", err)
	}
	if _, err := reader.GetDiagnostics(harness.ctx, otherProject, otherPrincipal, run.ID); !errors.Is(err, runtimepkg.ErrRunNotFound) {
		t.Fatalf("cross-project diagnostics read error=%v, want not found", err)
	}
	if _, err := reader.GetRun(harness.ctx, harness.projectID, otherPrincipal, run.ID); err == nil {
		t.Fatal("principal outside the Project read a Run")
	}
	if run.DeadlineAt.Before(time.Now().UTC().Add(-time.Minute)) {
		t.Fatal("invalid fixture deadline")
	}
}
