package main

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"
)

const (
	// whiteoutPrefix marks a name that a layer removes from the layers below it.
	whiteoutPrefix = ".wh."
	// opaqueMarker marks a directory whose entries from lower layers are all removed.
	opaqueMarker = ".wh..wh..opq"
	// reservedPrefix names storage driver metadata that is not image content.
	reservedPrefix = ".wh..wh."
	// maxFileBytes bounds one regular file, which is held in memory.
	maxFileBytes = 1 << 30
	// maxLinkDepth bounds symbolic link resolution like the kernel does.
	maxLinkDepth = 40
)

const (
	permRead  fs.FileMode = 0o4
	permExec  fs.FileMode = 0o1
	classUser             = 6
	classGrp              = 3
)

// entry is one filesystem object with the ownership and mode recorded in the layer.
type entry struct {
	kind byte
	mode fs.FileMode
	uid  int
	gid  int
	link string
	data []byte
}

func (e *entry) isDir() bool     { return e.kind == tar.TypeDir }
func (e *entry) isRegular() bool { return e.kind == tar.TypeReg }

// credential is the identity a container process runs as.
type credential struct {
	uid int
	gid int
}

// allows reports whether the credential holds every wanted permission bit. Like
// the kernel it consults exactly one class: owner, then group, then others.
func (c credential) allows(e *entry, want fs.FileMode) bool {
	shift := 0
	switch {
	case c.uid == e.uid:
		shift = classUser
	case c.gid == e.gid:
		shift = classGrp
	}
	return (e.mode.Perm()>>shift)&want == want
}

// rootfs is the effective filesystem of an image: its layers applied in order.
type rootfs struct {
	entries map[string]*entry
}

func newRootfs() *rootfs {
	return &rootfs{entries: map[string]*entry{"/": {kind: tar.TypeDir, mode: 0o755}}}
}

// cleanPath confines a layer path to the image root.
func cleanPath(name string) string {
	return path.Clean("/" + name)
}

type change struct {
	name  string
	entry entry
}

// applyLayer merges one layer changeset. Whiteouts and opaque directories only
// remove what lower layers hold, so they are applied before the entries the
// layer itself supplies, whatever order the archive lists them in.
func (r *rootfs) applyLayer(layer io.Reader) error {
	reader := tar.NewReader(layer)
	var changes []change
	var removed, opaque []string
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read layer: %w", err)
		}
		name := cleanPath(header.Name)
		dir, base := path.Dir(name), path.Base(name)
		switch {
		case base == opaqueMarker:
			opaque = append(opaque, dir)
		case strings.HasPrefix(base, reservedPrefix):
		case strings.HasPrefix(base, whiteoutPrefix):
			target := strings.TrimPrefix(base, whiteoutPrefix)
			if target == "" {
				return fmt.Errorf("layer entry %s names no file to remove", name)
			}
			removed = append(removed, path.Join(dir, target))
		default:
			item, readErr := newEntry(header, reader)
			if readErr != nil {
				return fmt.Errorf("layer entry %s: %w", name, readErr)
			}
			changes = append(changes, change{name: name, entry: item})
		}
	}
	for _, dir := range opaque {
		r.removeChildren(dir)
	}
	for _, name := range removed {
		r.remove(name)
	}
	for _, item := range changes {
		if err := r.install(item); err != nil {
			return err
		}
	}
	return nil
}

func newEntry(header *tar.Header, body io.Reader) (entry, error) {
	item := entry{kind: header.Typeflag, mode: header.FileInfo().Mode(), uid: header.Uid, gid: header.Gid}
	switch header.Typeflag {
	case tar.TypeReg:
		data, err := io.ReadAll(io.LimitReader(body, maxFileBytes+1))
		if err != nil {
			return entry{}, fmt.Errorf("read content: %w", err)
		}
		if len(data) > maxFileBytes {
			return entry{}, fmt.Errorf("file exceeds %d bytes", maxFileBytes)
		}
		item.data = data
	case tar.TypeSymlink:
		item.link = header.Linkname
	case tar.TypeLink:
		item.link = cleanPath(header.Linkname)
	case tar.TypeDir, tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
	default:
		return entry{}, fmt.Errorf("unsupported type %q", header.Typeflag)
	}
	return item, nil
}

// install adds an entry the way a runtime unpacks it: missing parent directories
// appear as root-owned 0755 directories, a directory merges with the one below it
// and anything else replaces what it finds.
func (r *rootfs) install(item change) error {
	value := item.entry
	if value.kind == tar.TypeLink {
		target, ok := r.entries[value.link]
		if !ok || target.isDir() {
			return fmt.Errorf("hard link %s targets %s, which is not a file", item.name, value.link)
		}
		value = *target
	}
	if err := r.ensureParents(item.name); err != nil {
		return err
	}
	if existing, ok := r.entries[item.name]; ok && (!existing.isDir() || !value.isDir()) {
		r.remove(item.name)
	}
	r.entries[item.name] = &value
	return nil
}

func (r *rootfs) ensureParents(name string) error {
	current := "/"
	for _, part := range strings.Split(path.Dir(name), "/") {
		if part == "" {
			continue
		}
		current = path.Join(current, part)
		existing, ok := r.entries[current]
		switch {
		case !ok:
			r.entries[current] = &entry{kind: tar.TypeDir, mode: 0o755}
		case !existing.isDir():
			return fmt.Errorf("%s lies below %s, which is not a directory", name, current)
		}
	}
	return nil
}

// remove deletes a path and everything below it.
func (r *rootfs) remove(name string) {
	if name != "/" {
		delete(r.entries, name)
	}
	r.removeChildren(name)
}

func (r *rootfs) removeChildren(dir string) {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	for name := range r.entries {
		if name != dir && strings.HasPrefix(name, prefix) {
			delete(r.entries, name)
		}
	}
}

// children lists the names directly below a directory.
func (r *rootfs) children(dir string) []string {
	prefix := strings.TrimSuffix(dir, "/") + "/"
	var names []string
	for name := range r.entries {
		if name != dir && strings.HasPrefix(name, prefix) && !strings.Contains(name[len(prefix):], "/") {
			names = append(names, name[len(prefix):])
		}
	}
	return names
}

// lookup resolves a path inside the image. Symbolic links resolve within the
// image, never on the host. With a credential every directory on the way must
// be searchable by it; the final component is returned unfollowed when
// followFinal is false.
func (r *rootfs) lookup(name string, who *credential, followFinal bool) (*entry, error) {
	current := "/"
	pending := strings.Split(cleanPath(name), "/")
	links := 0
	for len(pending) > 0 {
		part := pending[0]
		pending = pending[1:]
		switch part {
		case "", ".":
			continue
		case "..":
			current = path.Dir(current)
			continue
		}
		directory := r.entries[current]
		if directory == nil || !directory.isDir() {
			return nil, fmt.Errorf("%s: %w", current, fs.ErrInvalid)
		}
		if who != nil && !who.allows(directory, permExec) {
			return nil, fmt.Errorf("%s is not searchable by %d:%d: %w", current, who.uid, who.gid, fs.ErrPermission)
		}
		next := path.Join(current, part)
		found := r.entries[next]
		if found == nil {
			return nil, fmt.Errorf("%s: %w", next, fs.ErrNotExist)
		}
		if found.kind == tar.TypeSymlink && (followFinal || len(pending) > 0) {
			links++
			if links > maxLinkDepth {
				return nil, fmt.Errorf("%s: too many levels of symbolic links", next)
			}
			if path.IsAbs(found.link) {
				current = "/"
			}
			pending = append(strings.Split(found.link, "/"), pending...)
			continue
		}
		current = next
	}
	return r.entries[current], nil
}
