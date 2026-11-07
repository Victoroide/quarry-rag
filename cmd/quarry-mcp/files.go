package main

import (
	"os"
	"path/filepath"
	"strings"
)

// uploadError rejects a file path with a message that is safe to return to the client.
type uploadError string

func (e uploadError) Error() string { return string(e) }

func openUpload(path string, roots []string) (*os.File, error) {
	resolved, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return nil, uploadError("file is unavailable")
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return nil, uploadError("file path is invalid")
	}
	switch strings.ToLower(filepath.Ext(resolved)) {
	case ".pdf", ".docx", ".doc", ".txt", ".md", ".markdown":
	default:
		return nil, uploadError("file extension must be pdf, docx, doc, txt, md or markdown")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() {
		return nil, uploadError("path must identify a regular file")
	}
	for _, root := range roots {
		canonicalRoot, err := filepath.EvalSymlinks(root)
		if err != nil {
			continue
		}
		canonicalRoot, err = filepath.Abs(canonicalRoot)
		if err != nil {
			continue
		}
		relative, err := filepath.Rel(canonicalRoot, resolved)
		if err != nil || filepath.IsAbs(relative) || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			continue
		}
		// Root.Open also prevents a symlink replacement from escaping during the open.
		directory, err := os.OpenRoot(canonicalRoot)
		if err != nil {
			continue
		}
		file, err := directory.Open(relative)
		_ = directory.Close()
		if err != nil {
			return nil, uploadError("file is unavailable")
		}
		openedInfo, err := file.Stat()
		if err != nil || !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
			_ = file.Close()
			return nil, uploadError("path must identify a regular file")
		}
		return file, nil
	}
	return nil, uploadError("file must be inside an allowed root")
}
