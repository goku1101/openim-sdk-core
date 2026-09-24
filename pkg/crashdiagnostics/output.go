// Package crashdiagnostics retains the runtime's fatal output for the next App start.
// It does not recover panics, replace signal handlers, or change traceback settings.
package crashdiagnostics

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"
)

var once sync.Once

// Configure is best-effort; diagnostics must not change SDK initialization results.
func Configure(appFilesDir string) {
	once.Do(func() { _ = install(appFilesDir) })
}

func install(root string) error {
	if root == "" {
		return fmt.Errorf("empty diagnostic directory")
	}
	dir := filepath.Join(root, "vlink-go-crashes")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var old []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "go-crash-") && strings.HasSuffix(entry.Name(), ".log") {
			old = append(old, entry.Name())
		}
	}
	sort.Strings(old)
	// Keep at most two older process reports, plus this process's output.
	for len(old) > 2 {
		_ = os.Remove(filepath.Join(dir, old[0]))
		old = old[1:]
	}
	name := fmt.Sprintf("go-crash-%019d-%d.log", time.Now().UnixNano(), os.Getpid())
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = debug.SetCrashOutput(f, debug.CrashOptions{}); err != nil {
		_ = os.Remove(f.Name())
		return err
	}
	return nil
}
