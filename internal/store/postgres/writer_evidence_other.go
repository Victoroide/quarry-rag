//go:build !unix

package postgres

import "os"

func writerEvidenceOutput(stderr *os.File) (*os.File, error) {
	return stderr, nil
}
