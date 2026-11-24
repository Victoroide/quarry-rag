//go:build unix

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGrantCatalogSignalDuringInput(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	var listenerConfig net.ListenConfig
	listener, err := listenerConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	connected := make(chan struct{}, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			connected <- struct{}{}
			_ = connection.Close()
		}
	}()

	binary, err := os.Executable()
	require.NoError(t, err)
	host, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	inheritedInput, input, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = inheritedInput.Close() }()
	defer func() { _ = input.Close() }()
	output, inheritedOutput, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = output.Close() }()
	defer func() { _ = inheritedOutput.Close() }()
	child, err := os.StartProcess(binary, []string{binary, "-test.run=^TestGrantCatalogInputProcess$"}, &os.ProcAttr{
		Env: append(os.Environ(), "QUARRY_GRANT_CATALOG_TEST_CHILD=1",
			"DATABASE_URL=host="+host+" port="+port+" user=operator dbname=catalog sslmode=disable"),
		Files: []*os.File{inheritedInput, inheritedOutput, inheritedOutput},
	})
	require.NoError(t, err)
	defer func() { _ = child.Kill() }()
	stop := context.AfterFunc(ctx, func() { _ = child.Kill() })
	defer stop()
	type exit struct {
		state *os.ProcessState
		err   error
	}
	waited := make(chan exit, 1)
	go func() {
		state, waitErr := child.Wait()
		waited <- exit{state, waitErr}
	}()
	require.NoError(t, inheritedInput.Close())
	require.NoError(t, inheritedOutput.Close())
	childOutput := bufio.NewReader(output)
	ready, err := childOutput.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "reading input\n", ready)
	_, err = fmt.Fprint(input, "{")
	require.NoError(t, err)
	select {
	case <-connected:
		t.Error("the command connected before reading and validating the batch")
	case <-time.After(250 * time.Millisecond):
	}
	require.NoError(t, child.Signal(syscall.SIGTERM))
	select {
	case result := <-waited:
		require.NoError(t, result.err)
		remainder, readErr := io.ReadAll(childOutput)
		require.NoError(t, readErr)
		require.True(t, result.state.Success(), string(remainder))
		require.Contains(t, string(remainder), "context canceled")
	case <-time.After(2 * time.Second):
		t.Fatal("the command remained blocked with stdin open after SIGTERM")
	}
	select {
	case <-connected:
		t.Fatal("the interrupted command reached the database")
	default:
	}
}

func TestGrantCatalogInputProcess(t *testing.T) {
	if os.Getenv("QUARRY_GRANT_CATALOG_TEST_CHILD") != "1" {
		return
	}
	ctx, stop := signal.NotifyContext(t.Context(), syscall.SIGTERM)
	defer stop()
	_, err := fmt.Fprintln(os.Stdout, "reading input")
	require.NoError(t, err)
	err = run(ctx, nil, os.Getenv("DATABASE_URL"), os.Stdin, os.Stdout)
	require.ErrorIs(t, err, context.Canceled)
	_, err = fmt.Fprintln(os.Stderr, err)
	require.NoError(t, err)
}
