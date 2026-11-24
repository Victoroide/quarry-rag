//go:build !unix

package pipeline

import "os"

const catalogOpenFlags = os.O_RDONLY
