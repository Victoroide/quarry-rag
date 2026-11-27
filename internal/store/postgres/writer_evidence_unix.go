//go:build unix

package postgres

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// Use a private descriptor so a closed stderr pipe reports EPIPE instead of
// terminating the process. The recovery stream owns it for the process lifetime.
func writerEvidenceOutput(stderr *os.File) (*os.File, error) {
	conn, err := stderr.SyscallConn()
	if err != nil {
		return nil, err
	}
	var fd int
	var duplicateErr error
	if controlErr := conn.Control(func(source uintptr) {
		// Reserve a nonstandard descriptor and keep it out of child processes.
		const firstNonStandardFD = 3
		syscall.ForkLock.RLock()
		defer syscall.ForkLock.RUnlock()
		fd, duplicateErr = unix.FcntlInt(source, unix.F_DUPFD, firstNonStandardFD)
		if duplicateErr == nil {
			unix.CloseOnExec(fd)
		}
	}); controlErr != nil {
		return nil, controlErr
	}
	if duplicateErr != nil {
		return nil, duplicateErr
	}
	return os.NewFile(uintptr(fd), stderr.Name()), nil
}
