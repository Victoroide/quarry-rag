package pgtest

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// sessionEnv is the variable the library reads for the identifier that names
// the reaper container and labels the containers the reaper cleans up.
const sessionEnv = "TESTCONTAINERS_SESSION_ID"

var isolateOnce sync.Once

// isolateReaper gives this test process a reaper of its own unless the caller
// chose a session with TESTCONTAINERS_SESSION_ID. It must run before the
// library reads its configuration, which happens when the first container is
// requested.
//
// The library derives the session from the parent process, so every test
// binary that one `go test` run starts shares a single reaper. A binary that
// finds the reaper stopped, or cannot connect to it, terminates the container
// for all of them, and the reaper exits by itself once no binary has held a
// container for the reconnection timeout. A binary that starts a container
// at that moment fails against a reaper that other binaries ended.
func isolateReaper() error {
	var err error
	isolateOnce.Do(func() {
		err = isolateSession(os.Getenv, os.Setenv, os.Getpid(), time.Now())
	})
	return err
}

func isolateSession(get func(string) string, set func(string, string) error, pid int, started time.Time) error {
	if get(sessionEnv) != "" {
		return nil
	}
	return set(sessionEnv, processSession(pid, started))
}

// processSession names the session of one test process. The process
// identifier and start time distinguish concurrent and successive processes.
func processSession(pid int, started time.Time) string {
	return fmt.Sprintf("quarry-%d-%d", pid, started.UnixNano())
}
