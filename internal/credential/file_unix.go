//go:build unix

package credential

import (
	"os"
	"syscall"
)

// Avoid a blocking open if a regular file is replaced by a pipe.
const readFlags = os.O_RDONLY | syscall.O_NONBLOCK

func hasDeniedPermissions(mode, denied os.FileMode) bool {
	return mode.Perm()&denied != 0
}
