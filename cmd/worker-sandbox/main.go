package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	workerprocess "github.com/uu999/evalfrog/internal/worker/process"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) > 1 && os.Args[1] == "--sandbox-runtime" {
		os.Exit(workerprocess.RunSandboxRuntime(ctx, os.Args[2:], os.Stdout, os.Stderr))
	}
	os.Exit(workerprocess.RunProcess(ctx, os.Args[1:], "sandbox", os.Stdout, os.Stderr))
}
