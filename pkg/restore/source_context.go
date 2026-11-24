package restore

import "context"

type retentionRequestKey struct{}
type retainedSourceKey struct{}

// RequestSourceRetention is for an in-process launcher only. No pointer or
// artifact capability crosses an RPC or an environment variable.
func RequestSourceRetention(ctx context.Context) context.Context {
	return context.WithValue(ctx, retentionRequestKey{}, true)
}
func SourceRetentionRequested(ctx context.Context) bool {
	v, _ := ctx.Value(retentionRequestKey{}).(bool)
	return v
}

// WithPreparedSources borrows one request's validated handles. The caller owns
// Close until the complete run finishes; Run still verifies source/key/policy
// and exact runtime identity before consuming the capability.
func WithPreparedSources(ctx context.Context, p *PreparedSources) context.Context {
	return context.WithValue(ctx, retainedSourceKey{}, p)
}
func PreparedSourcesFromContext(ctx context.Context) *PreparedSources {
	p, _ := ctx.Value(retainedSourceKey{}).(*PreparedSources)
	return p
}

// A shared prepared holder receives this single-assignment, request-owned
// capability after durable assignment. The task keeps it alive through VM exit.
type sourceHandoffKey struct{}

func WithSourceHandoff(ctx context.Context, ch <-chan *PreparedSources) context.Context {
	return context.WithValue(ctx, sourceHandoffKey{}, ch)
}
func SourceHandoff(ctx context.Context) <-chan *PreparedSources {
	ch, _ := ctx.Value(sourceHandoffKey{}).(<-chan *PreparedSources)
	return ch
}
