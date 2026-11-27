package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

func TestDiagnosticSpillBrokenPipeCleansUp(t *testing.T) {
	temp := t.TempDir()
	reader, writer, pipeErr := os.Pipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			t.Error(err)
		}
	}()
	cmd := exec.CommandContext(t.Context(), "/proc/self/exe", "-test.run=^TestDiagnosticSpillProcess$")
	cmd.Env = append(os.Environ(), reportProcess+"=1", "TMPDIR="+temp)
	cmd.Stdin = bytes.NewBufferString(stream(t,
		event{Action: "output", Package: "a", Test: "TestA", Output: "test output\n"},
		event{Action: "pass", Package: "a", Test: "TestA"},
		event{Action: "pass", Package: "a"},
	))
	cmd.Stdout, cmd.Stderr = writer, io.Discard
	var exited *exec.ExitError
	if err := cmd.Run(); !errors.As(err, &exited) || exited.ExitCode() != 1 {
		t.Fatalf("broken pipe exit = %v", err)
	}
	files, err := os.ReadDir(temp)
	if err != nil || len(files) != 0 {
		t.Fatalf("temporary files after broken pipe: %v, %v", files, err)
	}
}

// BenchmarkDiagnosticSpillPeakRSS measures each input in a fresh Linux
// process. The parent generates the input and the child discards the output.
func BenchmarkDiagnosticSpillPeakRSS(b *testing.B) {
	const maxRSSIncreaseKiB = 32 * 1024
	var small, large int64
	for b.Loop() {
		small = diagnosticRSS(b, 16)
		large = diagnosticRSS(b, 128)
		if large-small > maxRSSIncreaseKiB {
			b.Fatalf("peak RSS increased by %d KiB (16 MiB stream: %d KiB, 128 MiB stream: %d KiB)",
				large-small, small, large)
		}
		verifyReusedDiagnosticVolume(b, 1536)
	}
	b.ReportMetric(float64(small), "KiB_RSS_16MiB")
	b.ReportMetric(float64(large), "KiB_RSS_128MiB")
	b.ReportMetric(float64(large-small), "KiB_RSS_delta")
}

func diagnosticRSS(b *testing.B, mebibytes int) int64 {
	b.Helper()
	cmd := exec.CommandContext(b.Context(), "/proc/self/exe", "-test.run=^TestDiagnosticSpillProcess$")
	cmd.Env = append(os.Environ(), reportProcess+"=1")
	input := newVolumeReader(mebibytes, false)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = input, io.Discard, io.Discard
	var exited *exec.ExitError
	if err := cmd.Run(); !errors.As(err, &exited) || exited.ExitCode() != 1 ||
		input.emitted != input.chunks || input.line.Len() != 0 {
		b.Fatalf("%d MiB reporter subprocess: %v, consumed %d/%d payload chunks", mebibytes, err, input.emitted, input.chunks)
	}
	return cmd.ProcessState.SysUsage().(*syscall.Rusage).Maxrss
}
