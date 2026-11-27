//go:build !darwin && !linux

package postgres

import "os"

func publishRecallFile(from, to string) error {
	// Atomically publish the completed file where an exclusive rename
	// primitive is unavailable. The caller removes the temporary name.
	return os.Link(from, to)
}
