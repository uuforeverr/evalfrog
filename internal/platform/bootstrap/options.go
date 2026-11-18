package bootstrap

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/uu999/evalfrog/internal/platform/config"
)

type Options struct {
	Profile        string
	ConfigDir      string
	CheckConfig    bool
	Migrate        bool
	HealthcheckURL string
}

func Parse(service string, arguments []string, output io.Writer) (Options, error) {
	flags := flag.NewFlagSet(service, flag.ContinueOnError)
	flags.SetOutput(output)
	var options Options
	flags.StringVar(&options.Profile, "profile", "", "configuration profile: local, test, or production-default")
	flags.StringVar(&options.ConfigDir, "config-dir", "", "directory containing EvalFrog profile YAML files")
	flags.BoolVar(&options.CheckConfig, "check-config", false, "validate configuration and exit")
	flags.BoolVar(&options.Migrate, "migrate", false, "apply MySQL migrations and exit")
	flags.StringVar(&options.HealthcheckURL, "healthcheck-url", "", "probe an HTTP readiness URL and exit")
	if err := flags.Parse(arguments); err != nil {
		return Options{}, err
	}
	if flags.NArg() != 0 {
		return Options{}, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	return options, nil
}

func Probe(ctx context.Context, target string) error {
	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("create healthcheck request: %w", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("healthcheck request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck returned %s", response.Status)
	}
	return nil
}

func Load(options Options) (config.Config, error) {
	return config.Load(config.LoadOptions{Directory: options.ConfigDir, Profile: options.Profile})
}
