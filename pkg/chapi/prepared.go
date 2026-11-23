package chapi

import (
	"context"
	"encoding/json"
)

func (c Client) PrepareContext(ctx context.Context, config json.RawMessage) error {
	return c.doContext(ctx, "PUT", "/api/v1/vmm.prepare", string(config))
}
func (c Client) RestoreCoWContext(ctx context.Context, source string) error {
	b, _ := json.Marshal(map[string]any{"source_url": source, "memory_restore_mode": "CopyOnWrite", "resume": false})
	return c.doContext(ctx, "PUT", "/api/v1/vm.restore", string(b))
}
