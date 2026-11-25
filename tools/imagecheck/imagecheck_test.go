package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"debug/elf"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	stdNotice   = "std@go1.test/LICENSE"
	depNotice   = "example.test/dependency@v1.0.0/LICENSE"
	otherNotice = "example.test/other@v1.0.0/LICENSE"
)

//go:embed testdata/ca.pem
var testCABundle string

//go:embed testdata/UTC
var testUTC string

type testFile struct {
	header tar.Header
	text   string
}

func testRegular(name string, mode int64, text string) testFile {
	return testFile{tar.Header{Name: name, Mode: mode, Typeflag: tar.TypeReg, Size: int64(len(text))}, text}
}

func testDirectory(name string, mode int64) testFile {
	return testFile{header: tar.Header{Name: name, Mode: mode, Typeflag: tar.TypeDir}}
}

func testELF(arch string) string {
	return testProgram(arch, elf.ET_EXEC, 0, "")
}

func testProgram(arch string, kind elf.Type, segment uint32, contents string) string {
	const (
		base        = 0x400000
		headerSize  = 64
		programSize = 56
	)
	// These Linux entry points call exit(0) using the target's syscall ABI.
	code := "\x31\xff\xb8\x3c\x00\x00\x00\x0f\x05"
	if arch == "arm64" {
		code = "\x00\x00\x80\xd2\xa8\x0b\x80\xd2\x01\x00\x00\xd4"
	}
	programs := []elf.Prog64{{Type: uint32(elf.PT_LOAD), Flags: uint32(elf.PF_R | elf.PF_X), Vaddr: base, Align: 0x10000}}
	programCount := uint16(1)
	if segment != 0 {
		programs = append(programs, elf.Prog64{Type: segment, Flags: uint32(elf.PF_R), Align: 1})
		programCount++
	}
	header := elf.Header64{
		Type: uint16(kind), Machine: uint16(machines[arch]), Version: uint32(elf.EV_CURRENT),
		Ehsize: headerSize, Phentsize: programSize, Phnum: programCount, Phoff: headerSize,
	}
	copy(header.Ident[:], "\x7fELF\x02\x01\x01")
	codeOffset := headerSize + programSize*uint64(programCount)
	header.Entry = base + codeOffset
	contentsOffset := codeOffset + uint64(len(code))
	programs[0].Filesz = contentsOffset + uint64(len(contents))
	programs[0].Memsz = programs[0].Filesz
	if segment != 0 {
		programs[1].Off = contentsOffset
		programs[1].Vaddr = base + contentsOffset
		programs[1].Filesz = uint64(len(contents))
		programs[1].Memsz = programs[1].Filesz
	}
	var data bytes.Buffer
	if err := binary.Write(&data, binary.LittleEndian, header); err != nil {
		panic(err)
	}
	if err := binary.Write(&data, binary.LittleEndian, programs); err != nil {
		panic(err)
	}
	return data.String() + code + contents
}

type fixture struct {
	config imageConfig
	files  map[string]testFile
	extra  []testFile
}

func newFixture(arch, license string, notices map[string]string) *fixture {
	cfg := imageConfig{platform: platform{OS: "linux", Architecture: arch}}
	cfg.Config.User = "10001:10001"
	cfg.Config.Entrypoint = []string{"/app/server"}
	files := map[string]testFile{
		"app":                               testDirectory("app", 0o755),
		"app/server":                        testRegular("app/server", 0o755, testELF(arch)),
		"etc/passwd":                        testRegular("etc/passwd", 0o644, "app:x:10001:10001::/home/app:/bin/false\n"),
		"etc/group":                         testRegular("etc/group", 0o644, "app:x:10001:app\n"),
		"etc/ssl/certs/ca-certificates.crt": testRegular("etc/ssl/certs/ca-certificates.crt", 0o644, testCABundle),
		"usr/share/zoneinfo/UTC":            testRegular("usr/share/zoneinfo/UTC", 0o644, testUTC),
	}
	project := "usr/share/licenses/quarry-rag/LICENSE"
	files[project] = testRegular(project, 0o644, license)
	for name, contents := range notices {
		name = "usr/share/licenses/quarry-rag/" + name
		files[name] = testRegular(name, 0o644, contents)
	}
	return &fixture{config: cfg, files: files}
}

func writeTestTar(t *testing.T, files []testFile) []byte {
	t.Helper()
	var body bytes.Buffer
	writer := tar.NewWriter(&body)
	for _, file := range files {
		header := file.header
		require.NoError(t, writer.WriteHeader(&header))
		_, err := writer.Write([]byte(file.text))
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	return body.Bytes()
}

func makeArchive(t *testing.T, nested bool, fixtures ...*fixture) string {
	t.Helper()
	files := []testFile{testRegular("oci-layout", 0o644, `{"imageLayoutVersion":"1.0.0"}`)}
	blob := func(media string, data []byte) descriptor {
		sum := fmt.Sprintf("%x", sha256.Sum256(data))
		files = append(files, testRegular("blobs/sha256/"+sum, 0o644, string(data)))
		return descriptor{MediaType: media, Digest: "sha256:" + sum}
	}
	document := func(media string, value any) descriptor {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		return blob(media, data)
	}
	layer := func(entries []testFile) descriptor {
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		_, err := writer.Write(writeTestTar(t, entries))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		return blob(mediaLayerGzip, compressed.Bytes())
	}

	var list index
	for _, f := range fixtures {
		entries := make([]testFile, 0, len(f.files))
		for _, file := range f.files {
			entries = append(entries, file)
		}
		layers := []descriptor{layer(entries)}
		if len(f.extra) > 0 {
			layers = append(layers, layer(f.extra))
		}
		img := document(mediaManifest, manifest{Config: document("application/vnd.oci.image.config.v1+json", f.config), Layers: layers})
		img.Platform = &platform{OS: "linux", Architecture: f.config.Architecture}
		list.Manifests = append(list.Manifests, img)
	}
	if nested {
		list.Manifests = append(list.Manifests, descriptor{MediaType: mediaManifest, Annotations: map[string]string{attestationKey: attestationValue}})
		list = index{Manifests: []descriptor{document(mediaIndex, list)}}
	}
	indexData, err := json.Marshal(list)
	require.NoError(t, err)
	files = append(files, testRegular("index.json", 0o644, string(indexData)))
	archivePath := filepath.Join(t.TempDir(), "image.tar")
	require.NoError(t, os.WriteFile(archivePath, writeTestTar(t, files), 0o600))
	return archivePath
}

func writeExpectations(t *testing.T, license string, notices map[string]string) (project, dir string) {
	t.Helper()
	dir = t.TempDir()
	project = filepath.Join(dir, "LICENSE")
	require.NoError(t, os.WriteFile(project, []byte(license), 0o600))
	for _, arch := range []string{"amd64", "arm64"} {
		for name, contents := range notices {
			file := filepath.Join(dir, "linux_"+arch, filepath.FromSlash(name))
			require.NoError(t, os.MkdirAll(filepath.Dir(file), 0o750))
			require.NoError(t, os.WriteFile(file, []byte(contents), 0o600))
		}
	}
	return project, dir
}

func TestVerifyEffectiveImage(t *testing.T) {
	const (
		license     = "project license\n"
		server      = "app/server"
		noticesRoot = "usr/share/licenses/quarry-rag/"
		certBundle  = "etc/ssl/certs/ca-certificates.crt"
		utcZone     = "usr/share/zoneinfo/UTC"
	)
	notices := map[string]string{stdNotice: "Go license\n", depNotice: "dependency license\n", otherNotice: "other dependency license\n"}
	project, expected := writeExpectations(t, license, notices)
	add := func(f *fixture, name string, mode int64, text string) { f.files[name] = testRegular(name, mode, text) }
	cases := map[string]struct {
		mutate func(*fixture)
		want   string
	}{
		"control":              {},
		"root-only executable": {func(f *fixture) { add(f, server, 0o700, testELF("arm64")) }, "cannot be executed as 10001:10001"},
		"set-user-ID executable": {func(f *fixture) {
			add(f, server, 0o4755, testELF("arm64"))
		}, "/app/server has set-user-ID or set-group-ID permissions"},
		"set-group-ID executable": {func(f *fixture) {
			add(f, server, 0o2755, testELF("arm64"))
		}, "/app/server has set-user-ID or set-group-ID permissions"},
		"root-only parent": {func(f *fixture) { f.files["app"] = testDirectory("app", 0o700) }, "/app is not searchable"},
		"group execute": {func(f *fixture) {
			file := testRegular(server, 0o710, testELF("arm64"))
			file.header.Gid = 10001
			f.files[server] = file
		}, ""},
		"ordinary license removal":    {func(f *fixture) { f.extra = []testFile{testRegular(noticesRoot+".wh.LICENSE", 0o644, "")} }, "missing notices: LICENSE"},
		"ordinary executable removal": {func(f *fixture) { f.extra = []testFile{testRegular("app/.wh.server", 0o644, "")} }, "executable /app/server is missing"},
		"ordinary executable replacement": {func(f *fixture) {
			f.extra = []testFile{testRegular("app/.wh.server", 0o644, ""), f.files[server]}
		}, ""},
		"opaque notice removal": {func(f *fixture) {
			f.extra = []testFile{testRegular(noticesRoot+".wh..wh..opq", 0o644, "")}
		}, "missing notices: LICENSE"},
		"opaque notice replacement": {func(f *fixture) {
			for name, text := range notices {
				f.extra = append(f.extra, testRegular(noticesRoot+name, 0o644, text))
			}
			f.extra = append(f.extra, f.files[noticesRoot+"LICENSE"], testRegular(noticesRoot+".wh..wh..opq", 0o644, ""))
			add(f, noticesRoot+"obsolete/NOTICE", 0o644, "old notice")
		}, ""},
		"missing single dependency": {func(f *fixture) { delete(f.files, noticesRoot+depNotice) }, "missing notices: " + depNotice},
		"empty standard and dependency licenses": {func(f *fixture) {
			add(f, noticesRoot+stdNotice, 0o644, "")
			add(f, noticesRoot+depNotice, 0o644, "")
		}, "notices differing from the build inputs: " + depNotice + ", " + stdNotice},
		"changed dependency bytes": {func(f *fixture) { add(f, noticesRoot+depNotice, 0o644, "unrelated") }, "notices differing from the build inputs: " + depNotice},
		"extra notice":             {func(f *fixture) { add(f, noticesRoot+"extra", 0o644, "unexpected") }, "unexpected notices: extra"},
		"altered project license":  {func(f *fixture) { add(f, noticesRoot+"LICENSE", 0o644, "another license") }, "notices differing from the build inputs: LICENSE"},
		"wrong architecture":       {func(f *fixture) { add(f, server, 0o755, testELF("amd64")) }, "executable targets amd64, want arm64"},
		"32-bit executable": {func(f *fixture) {
			program := []byte(testELF("arm64"))
			program[4] = 1
			add(f, server, 0o755, string(program))
		}, "not a 64-bit little-endian ELF"},
		"script executable":    {func(f *fixture) { add(f, server, 0o755, "#!/bin/sh\nexit 0\n") }, "not a 64-bit little-endian ELF"},
		"wrong user":           {func(f *fixture) { f.config.Config.User = "root" }, "runs as 'root'"},
		"wrong entry point":    {func(f *fixture) { f.config.Config.Entrypoint = []string{"/bin/sh"} }, "entry point"},
		"default command":      {func(f *fixture) { f.config.Config.Cmd = []string{"debug"} }, "default command"},
		"extra app file":       {func(f *fixture) { add(f, "app/debug", 0o644, "") }, "unexpected files in /app"},
		"toolchain installed":  {func(f *fixture) { add(f, "usr/local/go/bin/go", 0o755, "") }, "build toolchain"},
		"missing certificates": {func(f *fixture) { delete(f.files, certBundle) }, "CA certificates"},
		"truncated certificate": {func(f *fixture) {
			add(f, certBundle, 0o644, "-----BEGIN CERTIFICATE-----\n")
		}, "CA bundle contains no valid certificates"},
		"missing timezone data": {func(f *fixture) { delete(f.files, utcZone) }, "timezone data"},
		"truncated timezone": {func(f *fixture) {
			add(f, utcZone, 0o644, testUTC[:5])
		}, "invalid UTC timezone data"},
		"missing passwd": {func(f *fixture) { delete(f.files, "etc/passwd") }, "/etc/passwd lacks"},
		"missing group":  {func(f *fixture) { delete(f.files, "etc/group") }, "/etc/group lacks"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture("arm64", license, notices)
			if tc.mutate != nil {
				tc.mutate(f)
			}
			passed, failed, err := verify(makeArchive(t, false, f), []string{"linux/arm64"}, project, expected)
			require.NoError(t, err)
			if tc.want == "" {
				require.Empty(t, failed)
				require.Len(t, passed, 1)
				return
			}
			require.Empty(t, passed)
			require.NotEmpty(t, failed)
			require.Contains(t, failed[0], tc.want)
		})
	}
}

func TestVerifyStaticEntrypoint(t *testing.T) {
	const license = "project license"
	const loader = "/lib/ld-linux-aarch64.so.1"
	notices := map[string]string{stdNotice: "Go license", depNotice: "dependency license"}
	project, expected := writeExpectations(t, license, notices)
	static := testELF("arm64")
	outside := []byte(static)
	var header elf.Header64
	_, err := binary.Decode(outside, binary.LittleEndian, &header)
	require.NoError(t, err)
	header.Entry += uint64(len(static))
	_, err = binary.Encode(outside, binary.LittleEndian, header)
	require.NoError(t, err)
	dynamic, err := binary.Append(nil, binary.LittleEndian, []elf.Dyn64{{Tag: int64(elf.DT_NEEDED)}, {Tag: int64(elf.DT_NULL)}})
	require.NoError(t, err)
	cases := []struct {
		name, executable, problem string
	}{
		{"static control", static, ""},
		{"20-byte prefix", static[:20], "ELF header"},
		{"truncated program table", static[:binary.Size(elf.Header64{})], "program header table"},
		{"truncated load segment", static[:len(static)-1], "load segment"},
		{"entry outside load segment", string(outside), "entry point is outside executable ELF file content"},
		{"PIE with absent loader", testProgram("arm64", elf.ET_DYN, uint32(elf.PT_INTERP), loader+"\x00"), "requires an interpreter"},
		{"shared dependency without section headers", testProgram("arm64", elf.ET_DYN, uint32(elf.PT_DYNAMIC), string(dynamic)), "requires a shared library"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture("arm64", license, notices)
			f.files["app/server"] = testRegular("app/server", 0o755, tc.executable)
			passed, failed, err := verify(makeArchive(t, false, f), []string{"linux/arm64"}, project, expected)
			require.NoError(t, err)
			if tc.problem == "" {
				require.Empty(t, failed)
				require.Len(t, passed, 1)
				return
			}
			require.Empty(t, passed)
			require.Len(t, failed, 1)
			require.Contains(t, failed[0], tc.problem)
		})
	}
}

func TestVerifyPlatformSet(t *testing.T) {
	license := "project license"
	notices := map[string]string{stdNotice: "Go license", depNotice: "dependency license"}
	project, expected := writeExpectations(t, license, notices)
	amd64, arm64 := newFixture("amd64", license, notices), newFixture("arm64", license, notices)
	armOnly := "example.test/arm-only@v1.0.0/LICENSE"
	armContent := "arm64 dependency license"
	armOnlyPath := filepath.Join(expected, "linux_arm64", filepath.FromSlash(armOnly))
	require.NoError(t, os.MkdirAll(filepath.Dir(armOnlyPath), 0o750))
	require.NoError(t, os.WriteFile(armOnlyPath, []byte(armContent), 0o600))
	armNoticePath := "usr/share/licenses/quarry-rag/" + armOnly
	arm64.files[armNoticePath] = testRegular(armNoticePath, 0o644, armContent)
	platforms := []string{"linux/amd64", "linux/arm64"}
	valid := makeArchive(t, true, arm64, amd64)
	passed, failed, err := verify(valid, platforms, project, expected)
	require.NoError(t, err)
	require.Empty(t, failed)
	require.Len(t, passed, 2)

	_, failed, err = verify(makeArchive(t, false, amd64, amd64), platforms, project, expected)
	require.NoError(t, err)
	require.Equal(t, []string{"duplicate platform linux/amd64", "missing platform linux/arm64"}, failed)
	_, failed, err = verify(valid, platforms[:1], project, expected)
	require.NoError(t, err)
	require.Equal(t, []string{"unexpected platform linux/arm64"}, failed)
	_, _, err = verify(valid, []string{"linux/386"}, project, expected)
	require.ErrorContains(t, err, "unsupported platform")

	arm64.config.Variant = "v8"
	_, failed, err = verify(makeArchive(t, false, arm64), platforms[1:], project, expected)
	require.NoError(t, err)
	require.Equal(t, []string{"linux/arm64: configuration describes linux/arm64/v8"}, failed)
}

func TestVerifyRequiresOCIArchive(t *testing.T) {
	project, expected := writeExpectations(t, "project license", map[string]string{stdNotice: "Go license"})
	verifyArchive := func(name string) error {
		_, _, err := verify(name, []string{"linux/amd64"}, project, expected)
		return err
	}
	dir := t.TempDir()
	require.ErrorContains(t, verifyArchive(filepath.Join(dir, "missing.tar")), "cannot read")
	invalid := filepath.Join(dir, "invalid.tar")
	require.NoError(t, os.WriteFile(invalid, []byte("not a tar archive"), 0o600))
	require.ErrorContains(t, verifyArchive(invalid), "cannot unpack")
	empty := filepath.Join(dir, "empty.tar")
	require.NoError(t, os.WriteFile(empty, writeTestTar(t, nil), 0o600))
	require.ErrorContains(t, verifyArchive(empty), "not an image layout")
}
