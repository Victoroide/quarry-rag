package main

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func lockLedgerFile(file *os.File) error {
	return windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
		0, 1, 0, new(windows.Overlapped))
}

func replaceLedgerFile(root *os.Root, from, to string) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	parent, err := ledgerDirectoryPath(windows.Handle(dir.Fd()))
	if err != nil {
		return err
	}
	// MoveFileEx requires paths. Use the opened directory's resolved path for
	// both names and keep the root open during the write-through replacement.
	src, err := windows.UTF16PtrFromString(filepath.Join(parent, from))
	if err != nil {
		return err
	}
	dst, err := windows.UTF16PtrFromString(filepath.Join(parent, to))
	if err != nil {
		return err
	}
	if err := windows.MoveFileEx(src, dst, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH); err != nil {
		return &os.LinkError{Op: "replace ledger", Old: from, New: to, Err: err}
	}
	return nil
}

func ledgerDirectoryPath(dir windows.Handle) (string, error) {
	path := make([]uint16, windows.MAX_PATH)
	for {
		n, err := windows.GetFinalPathNameByHandle(dir, &path[0], uint32(len(path)), 0)
		if err != nil {
			return "", err
		}
		if n < uint32(len(path)) {
			return windows.UTF16ToString(path[:n]), nil
		}
		path = make([]uint16, n)
	}
}
