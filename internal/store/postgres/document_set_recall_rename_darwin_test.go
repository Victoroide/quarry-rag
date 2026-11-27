package postgres

import "golang.org/x/sys/unix"

func publishRecallFile(from, to string) error {
	return unix.RenamexNp(from, to, unix.RENAME_EXCL)
}
