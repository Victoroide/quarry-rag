//go:build unix

package postgres

import (
	"os"

	"golang.org/x/sys/unix"
)

func openRecallInput(root *os.Root, name string) (*os.File, error) {
	// A FIFO substituted after the path check must open without a writer.
	return root.OpenFile(name, os.O_RDONLY|unix.O_NONBLOCK|unix.O_NOCTTY, 0)
}
