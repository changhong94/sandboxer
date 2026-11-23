// Package instanceenv carries an immutable instance environment through host
// controller calls. It never installs tenant values in the process environment.
package instanceenv

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/ingest"
)

type contextKey struct{}
type values map[string]string

// With replaces the complete instance environment, not a process-global overlay.
// Copying also prevents later edits to the caller's slice from changing a tenant.
func With(ctx context.Context, env []string) (context.Context, error) {
	v := values{}
	for _, item := range env {
		k, x, ok := strings.Cut(item, "=")
		if !ok || k == "" || strings.ContainsRune(k, 0) || strings.ContainsRune(x, 0) {
			return nil, fmt.Errorf("invalid instance environment")
		}
		if _, ok := v[k]; ok {
			return nil, fmt.Errorf("duplicate instance environment key")
		}
		v[k] = x
	}
	return context.WithValue(ctx, contextKey{}, v), nil
}
func Scoped(ctx context.Context) bool { _, ok := ctx.Value(contextKey{}).(values); return ok }
func Get(ctx context.Context, k string) string {
	if v, ok := ctx.Value(contextKey{}).(values); ok {
		return v[k]
	}
	return os.Getenv(k)
}

// CustomerKey captures the key binding of this instance. A scoped invocation
// must never fall back to another instance's MANIFEST_KEY in the host process.
func CustomerKey(ctx context.Context, cfg *manifest.Config) ingest.CustomerKeyFunc {
	if !Scoped(ctx) {
		if cfg == nil {
			cfg = &manifest.Config{}
		}
		return cfg.CustomerKey
	}
	raw := Get(ctx, manifest.CustomerKeyEnv)
	if raw == "" && cfg != nil {
		raw = cfg.Manifest.Key
	}
	return func() ([32]byte, error) {
		var key [32]byte
		b, err := hex.DecodeString(raw)
		if err != nil || len(b) != len(key) {
			return key, fmt.Errorf("instance customer key must be 32 hex-encoded bytes")
		}
		copy(key[:], b)
		clear(b)
		return key, nil
	}
}

// ManifestConfig preserves CLI precedence without reading another instance's
// process-level MANIFEST_CONFIG when an explicit scope is installed.
func ManifestConfig(ctx context.Context, path string) (*manifest.Config, error) {
	if path == "" {
		path = Get(ctx, "MANIFEST_CONFIG")
	}
	if path == "" {
		return nil, manifest.ErrConfigNotProvided
	}
	return manifest.LoadConfig(path, "")
}

// Shared marks a runtime whose process also owns other VM instances.
type sharedKey struct{}

func WithShared(ctx context.Context) context.Context {
	return context.WithValue(ctx, sharedKey{}, true)
}
func Shared(ctx context.Context) bool { v, _ := ctx.Value(sharedKey{}).(bool); return v }

// Environ copies the instance environment for a private child/lease request.
func Environ(ctx context.Context) []string {
	if v, ok := ctx.Value(contextKey{}).(values); ok {
		out := make([]string, 0, len(v))
		for k, x := range v {
			out = append(out, k+"="+x)
		}
		return out
	}
	return os.Environ()
}
