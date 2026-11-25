//go:build !unix

package credential

import "os"

const readFlags = os.O_RDONLY

// The operator configures filesystem access controls on non-POSIX systems.
func hasDeniedPermissions(os.FileMode, os.FileMode) bool { return false }
