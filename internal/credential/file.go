// Package credential reads bounded local credential material and compact JWT data.
package credential

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

var errFile = errors.New("credential file unavailable or invalid")

// ReadFile reads a regular file with bounded size and permissions. The parent
// directory must be controlled by the operator. Symlinks must stay inside it.
func ReadFile(path string, limit int64, denied os.FileMode) ([]byte, error) {
	acceptable := func(info os.FileInfo) bool {
		return info.Mode().IsRegular() && !hasDeniedPermissions(info.Mode(), denied) && info.Size() <= limit
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, errFile
	}
	defer func() { _ = root.Close() }()
	name := filepath.Base(path)
	info, err := root.Stat(name)
	if err != nil || !acceptable(info) {
		return nil, errFile
	}
	file, err := root.OpenFile(name, readFlags, 0)
	if err != nil {
		return nil, errFile
	}
	defer func() { _ = file.Close() }()
	info, err = file.Stat()
	if err != nil || !acceptable(info) {
		return nil, errFile
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, errFile
	}
	return data, nil
}
