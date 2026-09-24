package crashdiagnostics

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestFatalOutputSurvivesProcess(t *testing.T) {
	if os.Getenv("VLINK_CRASH_CHILD") == "1" {
		Configure(os.Getenv("VLINK_CRASH_DIR"))
		Configure(os.Getenv("VLINK_CRASH_DIR"))
		panic("private-fixture-must-not-be-uploaded")
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestFatalOutputSurvivesProcess$")
	cmd.Env = append(os.Environ(), "VLINK_CRASH_CHILD=1", "VLINK_CRASH_DIR="+dir)
	if err := cmd.Run(); err == nil {
		t.Fatal("child must still crash")
	}
	files, err := filepath.Glob(filepath.Join(dir, "vlink-go-crashes", "go-crash-*.log"))
	if err != nil || len(files) != 1 {
		t.Fatalf("files=%v err=%v", files, err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "TestFatalOutputSurvivesProcess") || !strings.Contains(string(data), "panic:") {
		t.Fatal("missing original panic/stack")
	}
	info, _ := os.Stat(files[0])
	if info.Mode().Perm() != 0600 {
		t.Fatal("crash file must be private")
	}
}

func TestUnwritablePathDoesNotPanic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := install(path); err == nil {
		t.Fatal("expected local I/O failure")
	}
}
