package instanceenv

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestConcurrentInstanceKeysDoNotUseOrChangeHost(t *testing.T) {
	t.Setenv("MANIFEST_KEY", strings.Repeat("c", 64))
	t.Setenv("MANIFEST_CONFIG", "/host/private.yaml")
	a, _ := With(context.Background(), []string{"MANIFEST_KEY=" + strings.Repeat("a", 64), "MANIFEST_CONFIG=/a/config"})
	b, _ := With(context.Background(), []string{"MANIFEST_KEY=" + strings.Repeat("b", 64), "MANIFEST_CONFIG=/b/config"})
	ca, cancel := context.WithCancel(a)
	cancel()
	if b.Err() != nil {
		t.Fatal("peer canceled")
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, want, path := b, byte(0xbb), "/b/config"
			if i%2 == 0 {
				ctx, want, path = ca, 0xaa, "/a/config"
			}
			for j := 0; j < 100; j++ {
				key, err := CustomerKey(ctx, nil)()
				if err != nil || key[0] != want || Get(ctx, "MANIFEST_CONFIG") != path {
					t.Errorf("cross-instance state: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	if os.Getenv("MANIFEST_KEY") != strings.Repeat("c", 64) || os.Getenv("MANIFEST_CONFIG") != "/host/private.yaml" {
		t.Fatal("mutated host")
	}
	empty, _ := With(context.Background(), nil)
	if _, err := CustomerKey(empty, nil)(); err == nil {
		t.Fatal("fell back to host key")
	}
	if _, err := ManifestConfig(empty, ""); err == nil {
		t.Fatal("fell back to host config")
	}
}
func TestScopeIsCopiedAndRejectsMalformedInput(t *testing.T) {
	env := []string{"A=first"}
	ctx, err := With(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	env[0] = "A=second"
	if Get(ctx, "A") != "first" {
		t.Fatal("retained mutable input")
	}
	for _, v := range [][]string{{"A=1", "A=2"}, {"=x"}, {"missing"}, {"A=\x00"}} {
		if _, err := With(context.Background(), v); err == nil {
			t.Fatal("accepted invalid scope")
		}
	}
}
