//go:build unix

package pipeline

import (
	"os"
	"syscall"
)

// Opening a FIFO must return before the descriptor can be checked.
const catalogOpenFlags = os.O_RDONLY | syscall.O_NONBLOCK
