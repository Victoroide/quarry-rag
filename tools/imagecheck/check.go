package main

import (
	"crypto/sha256"
	"crypto/x509"
	"debug/elf"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	requiredUser  = "10001:10001"
	accountName   = "app"
	accountID     = "10001"
	serverPath    = "/app/server"
	appDirectory  = "/app"
	toolchainPath = "/usr/local/go"
	passwdPath    = "/etc/passwd"
	groupPath     = "/etc/group"
	bundlePath    = "/etc/ssl/certs/ca-certificates.crt"
	zonePath      = "/usr/share/zoneinfo/UTC"
	noticesRoot   = "/usr/share/licenses/quarry-rag"
	licenseName   = "LICENSE"
	// maxListed bounds how many files one notice difference names.
	maxListed = 10
)

// machines maps each supported architecture to its ELF machine type.
var machines = map[string]elf.Machine{"amd64": elf.EM_X86_64, "arm64": elf.EM_AARCH64}

var runtimeIdentity = credential{uid: 10001, gid: 10001}

// problems collects the failures found for one platform.
type problems struct {
	platform string
	list     []string
}

func (p *problems) addf(format string, args ...any) {
	p.list = append(p.list, p.platform+": "+fmt.Sprintf(format, args...))
}

// inventory maps a slash-separated notice path to the digest of its content.
type inventory map[string][sha256.Size]byte

// loadInventory reads every file below a directory.
func loadInventory(dir string) (inventory, error) {
	tree := os.DirFS(dir)
	found := inventory{}
	err := fs.WalkDir(tree, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("%s is not a regular file", name)
		}
		data, readErr := fs.ReadFile(tree, name)
		if readErr != nil {
			return readErr
		}
		found[name] = sha256.Sum256(data)
		return nil
	})
	return found, err
}

// expectation is what the build inputs say the image must carry for a platform.
type expectation struct {
	license []byte
	notices inventory
}

func supportedPlatform(value string) bool {
	parts := strings.Split(value, "/")
	if len(parts) != 2 || parts[0] != "linux" {
		return false
	}
	_, ok := machines[parts[1]]
	return ok
}

// checkImage verifies one image's configuration and effective filesystem.
func checkImage(a *archive, current image, want expectation, report *problems) {
	arch := current.platform.Architecture
	if current.config.platform != current.platform {
		report.addf("configuration describes %s", current.config.platform)
	}
	if current.config.Config.User != requiredUser {
		report.addf("runs as '%s', want %s", current.config.Config.User, requiredUser)
	}
	if !slices.Equal(current.config.Config.Entrypoint, []string{serverPath}) {
		report.addf("entry point is %q, want [%q]", current.config.Config.Entrypoint, serverPath)
	}
	if len(current.config.Config.Cmd) != 0 {
		report.addf("default command %q would pass arguments to the server", current.config.Config.Cmd)
	}

	root, err := a.filesystem(current)
	if err != nil {
		report.addf("%v", err)
		return
	}
	checkExecutable(root, arch, report)
	checkAccounts(root, report)
	checkRuntimeFiles(root, report)
	checkNotices(root, want, report)
}

func checkExecutable(root *rootfs, arch string, report *problems) {
	server, err := root.lookup(serverPath, &runtimeIdentity, false)
	switch {
	case errors.Is(err, fs.ErrPermission):
		report.addf("%s cannot be executed as %s: %v", serverPath, requiredUser, err)
		return
	case err != nil || server == nil:
		report.addf("executable %s is missing", serverPath)
		return
	case !server.isRegular():
		report.addf("%s is not a regular file", serverPath)
		return
	}
	if server.mode&(fs.ModeSetuid|fs.ModeSetgid) != 0 {
		report.addf("%s has set-user-ID or set-group-ID permissions", serverPath)
	}
	if !runtimeIdentity.allows(server, permExec) {
		report.addf("%s cannot be executed as %s: owner %d:%d, mode %04o", serverPath, requiredUser, server.uid, server.gid, server.mode.Perm())
	}
	if problem := checkELF(server.data, arch); problem != "" {
		report.addf("%s", problem)
	}
	var unexpected []string
	for _, name := range root.children(appDirectory) {
		if name != path.Base(serverPath) {
			unexpected = append(unexpected, name)
		}
	}
	if len(unexpected) > 0 {
		sort.Strings(unexpected)
		report.addf("unexpected files in %s: %s", appDirectory, strings.Join(unexpected, ", "))
	}
	if _, found := root.entries[toolchainPath]; found {
		report.addf("the build toolchain is present in the image")
	}
}

// checkELF reports why the executable is not a static ELF64 program for arch.
func checkELF(data []byte, arch string) string {
	if len(data) < elf.EI_NIDENT || string(data[:len(elf.ELFMAG)]) != elf.ELFMAG ||
		elf.Class(data[elf.EI_CLASS]) != elf.ELFCLASS64 || elf.Data(data[elf.EI_DATA]) != elf.ELFDATA2LSB {
		return "executable is not a 64-bit little-endian ELF file"
	}
	var header elf.Header64
	headerSize, err := binary.Decode(data, binary.LittleEndian, &header)
	if err != nil {
		return "executable has an incomplete ELF header"
	}
	if elf.Version(header.Ident[elf.EI_VERSION]) != elf.EV_CURRENT || header.Version != uint32(elf.EV_CURRENT) || int(header.Ehsize) != headerSize {
		return "executable has an invalid ELF header"
	}
	if kind := elf.Type(header.Type); kind != elf.ET_EXEC && kind != elf.ET_DYN {
		return "executable is not a program"
	}
	if machine := elf.Machine(header.Machine); machine != machines[arch] {
		target := fmt.Sprintf("machine %d", header.Machine)
		for name, candidate := range machines {
			if candidate == machine {
				target = name
			}
		}
		return fmt.Sprintf("executable targets %s, want %s", target, arch)
	}
	return checkELFSegments(data, header)
}

func checkELFSegments(data []byte, header elf.Header64) string {
	programSize := binary.Size(elf.Prog64{})
	fileSize := uint64(len(data))
	tableSize := uint64(header.Phnum) * uint64(header.Phentsize)
	if header.Phnum == 0 || int(header.Phentsize) != programSize ||
		header.Phoff < uint64(header.Ehsize) || header.Phoff > fileSize || tableSize > fileSize-header.Phoff {
		return "executable has an invalid ELF program header table"
	}

	// Linux loads from the program headers even when section headers are absent.
	entryMapped := false
	for table := data[header.Phoff : header.Phoff+tableSize]; len(table) > 0; table = table[programSize:] {
		var segment elf.Prog64
		if _, err := binary.Decode(table[:programSize], binary.LittleEndian, &segment); err != nil {
			return "executable has an invalid ELF program header"
		}
		switch elf.ProgType(segment.Type) {
		case elf.PT_INTERP:
			return "executable requires an interpreter"
		case elf.PT_LOAD:
			if segment.Off > fileSize || segment.Filesz > fileSize-segment.Off ||
				segment.Filesz > segment.Memsz || segment.Memsz > math.MaxUint64-segment.Vaddr {
				return "executable has an out-of-bounds ELF load segment"
			}
			if segment.Align > 1 && (segment.Align&(segment.Align-1) != 0 || segment.Off%segment.Align != segment.Vaddr%segment.Align) {
				return "executable has a misaligned ELF load segment"
			}
			if elf.ProgFlag(segment.Flags)&elf.PF_X != 0 && header.Entry >= segment.Vaddr && header.Entry-segment.Vaddr < segment.Filesz {
				entryMapped = true
			}
		case elf.PT_DYNAMIC:
			if segment.Off > fileSize || segment.Filesz > fileSize-segment.Off {
				return "executable has an out-of-bounds ELF dynamic segment"
			}
			if problem := checkELFDynamic(data[segment.Off : segment.Off+segment.Filesz]); problem != "" {
				return problem
			}
		}
	}
	if !entryMapped {
		return "entry point is outside executable ELF file content"
	}
	return ""
}

func checkELFDynamic(data []byte) string {
	entrySize := binary.Size(elf.Dyn64{})
	for ; len(data) >= entrySize; data = data[entrySize:] {
		switch binary.LittleEndian.Uint64(data) {
		case uint64(elf.DT_NULL):
			return ""
		case uint64(elf.DT_NEEDED), uint64(elf.DT_FILTER), uint64(elf.DT_AUXILIARY):
			return "executable requires a shared library"
		}
	}
	return "executable has an invalid ELF dynamic segment"
}

func readAsRoot(root *rootfs, name string) ([]byte, bool) {
	found, err := root.lookup(name, nil, true)
	if err != nil || found == nil || !found.isRegular() {
		return nil, false
	}
	return found.data, true
}

// hasAccount reports whether the file lists the runtime account with its numeric
// identifier in the given field of a colon-separated record.
func hasAccount(data []byte, field int) bool {
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.Split(line, ":")
		if len(parts) > field && parts[0] == accountName && parts[2] == accountID && parts[field] == accountID {
			return true
		}
	}
	return false
}

func checkAccounts(root *rootfs, report *problems) {
	passwd, ok := readAsRoot(root, passwdPath)
	if !ok || !hasAccount(passwd, 3) {
		report.addf("%s lacks the %s user with UID and GID %s", passwdPath, accountName, accountID)
	}
	group, ok := readAsRoot(root, groupPath)
	if !ok || !hasAccount(group, 2) {
		report.addf("%s lacks the %s group with GID %s", groupPath, accountName, accountID)
	}
}

func checkRuntimeFiles(root *rootfs, report *problems) {
	readable := func(name string) *entry {
		found, err := root.lookup(name, &runtimeIdentity, true)
		if err != nil || found == nil || !found.isRegular() {
			return nil
		}
		if !runtimeIdentity.allows(found, permRead) {
			return nil
		}
		return found
	}
	if bundle := readable(bundlePath); bundle == nil {
		report.addf("CA certificates are missing or unreadable by %s", requiredUser)
	} else if !x509.NewCertPool().AppendCertsFromPEM(bundle.data) {
		report.addf("CA bundle contains no valid certificates")
	}
	if zone := readable(zonePath); zone == nil {
		report.addf("timezone data is missing or unreadable by %s", requiredUser)
	} else if _, err := time.LoadLocationFromTZData("UTC", zone.data); err != nil {
		report.addf("invalid UTC timezone data: %v", err)
	}
}

// installedNotices lists the regular files below the notices directory.
func installedNotices(root *rootfs, report *problems) inventory {
	found := inventory{}
	prefix := noticesRoot + "/"
	for name, item := range root.entries {
		if !strings.HasPrefix(name, prefix) || item.isDir() {
			continue
		}
		if !item.isRegular() {
			report.addf("notice %s is not a regular file", strings.TrimPrefix(name, prefix))
			continue
		}
		found[strings.TrimPrefix(name, prefix)] = sha256.Sum256(item.data)
	}
	return found
}

// checkNotices requires the effective notice tree to equal the expected one:
// the project license plus the notices collected for this platform.
func checkNotices(root *rootfs, want expectation, report *problems) {
	expected := inventory{}
	for name, sum := range want.notices {
		expected[name] = sum
	}
	if _, clash := expected[licenseName]; clash {
		report.addf("the expected notices already contain %s", licenseName)
		return
	}
	expected[licenseName] = sha256.Sum256(want.license)
	installed := installedNotices(root, report)

	var missing, changed, extra []string
	for name, sum := range expected {
		got, present := installed[name]
		switch {
		case !present:
			missing = append(missing, name)
		case got != sum:
			changed = append(changed, name)
		}
	}
	for name := range installed {
		if _, listed := expected[name]; !listed {
			extra = append(extra, name)
		}
	}
	list := func(label string, names []string) {
		if len(names) == 0 {
			return
		}
		sort.Strings(names)
		more := ""
		if len(names) > maxListed {
			more = fmt.Sprintf(" and %d more", len(names)-maxListed)
			names = names[:maxListed]
		}
		report.addf("%s: %s%s", label, strings.Join(names, ", "), more)
	}
	list("missing notices", missing)
	list("notices differing from the build inputs", changed)
	list("unexpected notices", extra)
}
