package caddy_docker_upstreams

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bep/debounce"
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"go.uber.org/zap"
)

const (
	LabelEnable       = "com.caddyserver.http.enable"
	LabelUpstreamPort = "com.caddyserver.http.upstream.port"
)

func init() {
	caddy.RegisterModule(Upstreams{})
}

type candidate struct {
	matchers caddyhttp.MatcherSet
	upstream *reverseproxy.Upstream
}

var (
	candidates   []candidate
	candidatesMu sync.RWMutex
)

var defaultFilters = filters.NewArgs(
	filters.Arg("label", fmt.Sprintf("%s=true", LabelEnable)),
	filters.Arg("status", "running"), // types.ContainerState.Status
)

// Upstreams provides upstreams from the docker host.
type Upstreams struct{}

func (Upstreams) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.reverse_proxy.upstreams.docker",
		New: func() caddy.Module { return new(Upstreams) },
	}
}

func (u *Upstreams) provisionCandidates(ctx caddy.Context, cli *client.Client) error {
	containers, err := cli.ContainerList(ctx, container.ListOptions{Filters: defaultFilters})
	if err != nil {
		return fmt.Errorf("listing docker containers: %w", err)
	}

	updated := make([]candidate, 0, len(containers))

	for _, c := range containers {
		// Build matchers.
		matchers := buildMatchers(ctx, c.Labels)

		// Build upstream.
		port, ok := c.Labels[LabelUpstreamPort]
		if !ok {
			if len(c.Ports) == 0 {
				ctx.Logger().Error("unable to get port from container labels or config",
					zap.String("container_id", c.ID),
				)
				continue
			}

            port = fmt.Sprintf("%d", c.Ports[0].PublicPort)
		}

		updated = append(updated, candidate{
			matchers: matchers,
			upstream: &reverseproxy.Upstream{Dial: "localhost:" + port},
		})
	}

	candidatesMu.Lock()
	candidates = updated
	candidatesMu.Unlock()

	return nil
}

func (u *Upstreams) keepUpdated(ctx caddy.Context, cli *client.Client) {
	defer cli.Close()

	debounced := debounce.New(100 * time.Millisecond)

	for {
		messages, errs := cli.Events(ctx, events.ListOptions{
			Filters: filters.NewArgs(filters.Arg("type", string(events.ContainerEventType))),
		})

	selectLoop:
		for {
			select {
			case <-messages:
				debounced(func() {
					err := u.provisionCandidates(ctx, cli)
					if err != nil {
						ctx.Logger().Error("unable to provision the candidates", zap.Error(err))
					}
				})
			case err := <-errs:
				if errors.Is(err, context.Canceled) {
					return
				}

				ctx.Logger().Warn("unable to monitor container events; will retry", zap.Error(err))
				break selectLoop
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (u *Upstreams) provision(ctx caddy.Context, cli *client.Client) error {
	err := u.provisionCandidates(ctx, cli)
	if err != nil {
		return err
	}

	go u.keepUpdated(ctx, cli)

	return nil
}

func (u *Upstreams) Provision(ctx caddy.Context) error {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("provisioning docker client: %w", err)
	}

	ping, err := cli.Ping(ctx)
	if err != nil {
		return fmt.Errorf("ping docker server: %w", err)
	}
	ctx.Logger().Info("connected docker server", zap.String("api_version", ping.APIVersion))

	return u.provision(ctx, cli)
}

func (u *Upstreams) GetUpstreams(r *http.Request) ([]*reverseproxy.Upstream, error) {
	upstreams := make([]*reverseproxy.Upstream, 0, 1)

	candidatesMu.RLock()
	defer candidatesMu.RUnlock()

	for _, c := range candidates {
		if c.matchers.Match(r) {
			upstreams = append(upstreams, c.upstream)
		}
	}

	return upstreams, nil
}

// Interface guards
var (
	_ caddy.Provisioner           = (*Upstreams)(nil)
	_ reverseproxy.UpstreamSource = (*Upstreams)(nil)
)
