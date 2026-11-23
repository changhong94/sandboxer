package sandboxctl

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// Diagnostic phase names and timestamps only; never arguments or credentials.
func traceLaunchStages(op string) (func(string), func()) {
	if os.Getenv("CH_FORK_TIMING") == "" {
		return func(string) {}, func() {}
	}
	last := time.Now()
	var lines []string
	return func(name string) {
			now := time.Now()
			lines = append(lines, fmt.Sprintf("%s:%d:%d", name, last.UnixNano(), now.UnixNano()))
			last = now
		}, func() {
			if len(lines) == 0 {
				return
			}
			log.Printf("LAUNCH_STAGES op=%s spans=%s", op, strings.Join(lines, ","))
			lines = nil
		}
}
