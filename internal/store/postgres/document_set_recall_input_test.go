package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecallInputValidation(t *testing.T) {
	header := recallTestHeader()
	row := recallTestRecord("example")
	valid := header + "\n" + row + "\n"
	digest := sha256.Sum256([]byte(valid))
	for _, test := range []struct {
		name, input string
		count       int
		wantError   bool
	}{
		{"LF", valid, 1, false},
		{"CRLF", strings.ReplaceAll(valid, "\n", "\r\n"), 1, false},
		{"last line without terminator", strings.TrimSuffix(valid, "\n"), 1, false},
		{"malformed header", "{\n" + row, 1, true},
		{"duplicate header key", strings.Replace(header, "{", `{"version":1,`, 1) + "\n" + row, 1, true},
		{"unknown header field", strings.Replace(header, "{", `{"extra":1,`, 1) + "\n" + row, 1, true},
		{"version", strings.Replace(header, `"version":1`, `"version":2`, 1) + "\n" + row, 1, true},
		{"profile dimensions", strings.Replace(header, "768", "767", 1) + "\n" + row, 1, true},
		{"null header field", strings.Replace(header, `"encoder":"example"`, `"encoder":null`, 1) + "\n" + row, 1, true},
		{"truncated", header + "\n" + row[:len(row)-2], 1, true},
		{"too few rows", valid, 2, true},
		{"too many rows", valid + recallTestRecord("second"), 1, true},
		{"duplicate source ID", valid + row, 2, true},
		{"duplicate row key", header + "\n" + strings.Replace(row, "{", `{"k\u0065y":"duplicate",`, 1), 1, true},
		{"unknown row field", header + "\n" + strings.Replace(row, "{", `{"unknown":1,`, 1), 1, true},
		{"trailing token", valid + "{}{}", 2, true},
		{"invalid UTF-8", strings.Replace(valid, `"key":"example"`, "\"key\":\"\xff\"", 1), 1, true},
		{"unpaired surrogate", strings.Replace(valid, `"key":"example"`, `"key":"\uD800"`, 1), 1, true},
		{"valid surrogate pair", strings.Replace(valid, `"key":"example"`, `"key":"\uD83D\uDC3F"`, 1), 1, false},
		{"empty key", header + "\n" + recallTestRecord(""), 1, true},
		{"key at byte limit", header + "\n" + recallTestRecord(strings.Repeat("é", 128)), 1, false},
		{"key over byte limit", header + "\n" + recallTestRecord(strings.Repeat("é", 129)), 1, true},
		{"line at byte limit", header + "\n" + row + strings.Repeat(" ", recallLineBytes-len(row)) + "\r\n", 1, false},
		{"line over byte limit", header + "\n" + row + strings.Repeat(" ", recallLineBytes+1-len(row)) + "\n", 1, true},
		{"short vector", header + "\n" + `{"key":"a","embedding":[1]}`, 1, true},
		{"long vector", strings.Replace(valid, "[1,", "[1,1,", 1), 1, true},
		{"null component", strings.Replace(valid, "[1,", "[null,", 1), 1, true},
		{"quoted component", strings.Replace(valid, "[1,", `["1",`, 1), 1, true},
		{"NaN", strings.Replace(valid, "[1,", "[NaN,", 1), 1, true},
		{"infinity", strings.Replace(valid, "[1,", "[Infinity,", 1), 1, true},
		{"float32 overflow", strings.Replace(valid, "[1,", "[4e38,", 1), 1, true},
		{"zero vector", strings.Replace(valid, "[1,", "[0,", 1), 1, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := readRecallInput(t.Context(), strings.NewReader(test.input), test.count, nil)
			if test.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, result.keys, test.count)
			if test.name == "LF" {
				require.Equal(t, hex.EncodeToString(digest[:]), result.Digest)
			}
		})
	}
	a, err := readRecallInput(t.Context(), strings.NewReader(valid), 1, nil)
	require.NoError(t, err)
	b, err := readRecallInput(t.Context(), strings.NewReader(strings.Replace(valid, `"key":"example"`, `"key":"held-out"`, 1)), 1, nil)
	require.NoError(t, err)
	require.NoError(t, matchRecallInputs(a, b))
	require.ErrorContains(t, matchRecallInputs(a, a), "overlap")
	b.Profile.Encoder = "different"
	require.ErrorContains(t, matchRecallInputs(a, b), "profile")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = readRecallInput(ctx, recallFailReader{}, 1, nil)
	require.ErrorIs(t, err, context.Canceled)
	ctx, cancel = context.WithCancel(t.Context())
	t.Cleanup(cancel)
	_, err = readRecallInput(ctx, strings.NewReader(valid+recallTestRecord("next")), 2,
		func(_ int, _ [32]byte, _ []float32) error { cancel(); return nil })
	require.ErrorIs(t, err, context.Canceled)
}

func TestRecallCosineInputRange(t *testing.T) {
	dir := t.TempDir()
	for _, stream := range []string{"corpus.jsonl", "queries.jsonl"} {
		path := filepath.Join(dir, stream)
		for _, test := range []struct {
			name, components string
			want             []float32
		}{
			{"tiny single component", "1e-30", nil},
			{"huge single component", "1e30", nil},
			{"overflowing sum", "1.4e19,1.4e19", nil},
			{"subnormal squared norm", "1e-20", nil},
			{"reference scale", "0.03125,-0.0625,0.125", []float32{0.03125, -0.0625, 0.125}},
			{"normal vector with a tiny component", "1,-1e-30", []float32{1, -1e-30}},
		} {
			t.Run(stream+"/"+test.name, func(t *testing.T) {
				row := recallTestRecord("unpublished-source-key")
				row = strings.Replace(row, "[1"+strings.Repeat(",0", strings.Count(test.components, ",")), "["+test.components, 1)
				require.NoError(t, os.WriteFile(path, []byte(recallTestHeader()+"\n"+row), 0o600))
				var accepted []float32
				_, err := readRecallFile(t.Context(), path, 1, func(_ int, _ [32]byte, vec []float32) error {
					accepted = vec
					return nil
				})
				if test.want == nil {
					require.ErrorContains(t, err, "recall input row 1: recall embedding squared norm")
					require.NotContains(t, err.Error(), "unpublished-source-key")
					require.NotContains(t, err.Error(), test.components)
					require.Nil(t, accepted)
					return
				}
				require.NoError(t, err)
				require.Len(t, accepted, recallDimensions)
				want := make([]float32, recallDimensions)
				copy(want, test.want)
				require.Equal(t, want, accepted)
			})
		}
	}
}

func TestRecallInputPath(t *testing.T) {
	dir := t.TempDir()
	const (
		name       = "input.jsonl"
		invalid    = "invalid recall input filename"
		notRegular = "recall input must be a readable regular file"
		openFailed = "cannot open recall input"
	)
	path := filepath.Join(dir, name)
	digest := writeRecallTestInput(t, path, "example")

	t.Run("absolute", func(t *testing.T) {
		got, err := readRecallFile(t.Context(), path, 1, nil)
		require.NoError(t, err)
		require.Equal(t, digest, got.Digest)
	})
	t.Run("relative", func(t *testing.T) {
		t.Chdir(dir)
		got, err := readRecallFile(t.Context(), name, 1, nil)
		require.NoError(t, err)
		require.Equal(t, digest, got.Digest)
	})

	outside := filepath.Join(t.TempDir(), "outside.jsonl")
	writeRecallTestInput(t, outside, "outside")
	escape, err := filepath.Rel(dir, outside)
	require.NoError(t, err)
	for _, test := range []struct {
		name   string
		target string
	}{
		{"inside", name},
		{"escape", escape},
		{"absolute", path},
		{"dangling", "missing.jsonl"},
		{"loop", "loop-link"},
	} {
		t.Run("symlink/"+test.name, func(t *testing.T) {
			link := filepath.Join(dir, test.name+"-link")
			err := os.Symlink(test.target, link)
			if errors.Is(err, errors.ErrUnsupported) || (runtime.GOOS == "windows" && err != nil) {
				t.Skipf("cannot create symlink: %v", err)
			}
			require.NoError(t, err)
			got, err := readRecallFile(t.Context(), link, 1, nil)
			if test.name != "inside" {
				require.EqualError(t, err, notRegular)
				return
			}
			require.NoError(t, err)
			require.Equal(t, digest, got.Digest)
		})
	}

	for _, test := range []struct {
		name, path, wantErr string
	}{
		{"empty", "", invalid},
		{"dot", ".", invalid},
		{"trailing separator", path + string(filepath.Separator), invalid},
		{"directory", dir, notRegular},
		{"missing file", filepath.Join(dir, "absent.jsonl"), notRegular},
		{"missing directory", filepath.Join(dir, "missing", name), "cannot open recall input directory"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := readRecallFile(t.Context(), test.path, 1, nil)
			require.EqualError(t, err, test.wantErr)
		})
	}

	t.Run("permission denial", func(t *testing.T) {
		_, err := readRecallFileWithOpener(t.Context(), path, 1, nil, func(_ *os.Root, _ string) (*os.File, error) {
			return nil, &os.PathError{Op: "open", Path: path, Err: os.ErrPermission}
		})
		require.EqualError(t, err, openFailed)
	})
	t.Run("directory replacement", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), name)
		writeRecallTestInput(t, path, "example")
		_, err := readRecallFileWithOpener(t.Context(), path, 1, nil, func(root *os.Root, name string) (*os.File, error) {
			require.NoError(t, root.Remove(name))
			require.NoError(t, root.Mkdir(name, 0o700))
			return openRecallInput(root, name)
		})
		require.EqualError(t, err, notRegular)
	})
	t.Run("socket replacement", func(t *testing.T) {
		switch runtime.GOOS {
		case "js", "plan9", "wasip1", "windows":
			t.Skip("Unix filesystem socket replacement is tested on Unix")
		}
		dir := t.TempDir()
		t.Chdir(dir) // Keep the address within the Unix socket path limit.
		writeRecallTestInput(t, filepath.Join(dir, name), "example")
		_, err := readRecallFileWithOpener(t.Context(), name, 1, nil, func(root *os.Root, name string) (*os.File, error) {
			require.NoError(t, root.Remove(name))
			listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", name)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, listener.Close()) })
			return openRecallInput(root, name)
		})
		require.EqualError(t, err, openFailed)
	})

	t.Run("parent replacement", func(t *testing.T) {
		switch runtime.GOOS {
		case "js", "plan9", "windows":
			t.Skip("platform cannot track or rename an open root directory")
		}
		parent := t.TempDir()
		dir := filepath.Join(parent, "original")
		path := filepath.Join(dir, name)
		require.NoError(t, os.Mkdir(dir, 0o700))
		want := writeRecallTestInput(t, path, "original")
		got, err := readRecallFileWithOpener(t.Context(), path, 1, nil, func(root *os.Root, name string) (*os.File, error) {
			require.NoError(t, os.Rename(dir, filepath.Join(parent, "moved")))
			require.NoError(t, os.Mkdir(dir, 0o700))
			writeRecallTestInput(t, path, "replacement")
			return openRecallInput(root, name)
		})
		require.NoError(t, err)
		require.Equal(t, want, got.Digest)
	})
	t.Run("opened descriptor survives rename", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, name)
		want := writeRecallTestInput(t, path, "original")
		writeRecallTestInput(t, filepath.Join(dir, "replacement.jsonl"), "replacement")
		var keys [][32]byte
		got, err := readRecallFileWithOpener(t.Context(), path, 1, func(_ int, key [32]byte, _ []float32) error {
			keys = append(keys, key)
			return nil
		}, func(root *os.Root, name string) (*os.File, error) {
			file, err := openRecallInput(root, name)
			require.NoError(t, err)
			t.Cleanup(func() { _ = file.Close() })
			require.NoError(t, root.Rename(name, "moved.jsonl"))
			require.NoError(t, root.Rename("replacement.jsonl", name))
			return file, nil
		})
		require.NoError(t, err)
		require.Equal(t, want, got.Digest)
		require.Equal(t, [][32]byte{sha256.Sum256([]byte("original"))}, keys)
	})
}

func TestRecallInputOpenCancellation(t *testing.T) {
	rejectRows := func(t *testing.T) recallInputRow {
		return func(_ int, _ [32]byte, _ []float32) error {
			t.Fatal("unexpected parser callback")
			return nil
		}
	}
	t.Run("pre-canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := readRecallFileWithOpener(ctx, "", 1, rejectRows(t), func(_ *os.Root, _ string) (*os.File, error) {
			t.Fatal("unexpected open")
			return nil, nil
		})
		require.Equal(t, context.Canceled, err)
	})
	t.Run("canceled at opener", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "input.jsonl")
		writeRecallTestInput(t, path, "example")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var openedRoot *os.Root
		var openedFile *os.File
		_, err := readRecallFileWithOpener(ctx, path, 1, rejectRows(t), func(root *os.Root, _ string) (*os.File, error) {
			file, err := openRecallInput(root, ".")
			require.NoError(t, err)
			openedRoot, openedFile = root, file
			cancel()
			return file, nil
		})
		require.Equal(t, context.Canceled, err)
		_, err = openedFile.Stat()
		require.ErrorIs(t, err, os.ErrClosed)
		_, err = openedRoot.Stat(".")
		require.ErrorIs(t, err, os.ErrClosed)
	})
}

func writeRecallTestInput(t *testing.T, path, key string) string {
	t.Helper()
	content := []byte(recallTestHeader() + "\n" + recallTestRecord(key))
	require.NoError(t, os.WriteFile(path, content, 0o600))
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:])
}

type recallFailReader struct{}

func (recallFailReader) Read([]byte) (int, error) {
	return 0, errors.New("unexpected read")
}

var _ io.Reader = recallFailReader{}

func recallTestHeader() string {
	return fmt.Sprintf(`{"version":1,"source":"example","source_revision":"abcdef","encoder":"example","dimensions":%d}`, recallDimensions)
}

func recallTestRecord(key string) string {
	encoded, _ := json.Marshal(key)
	return `{"key":` + string(encoded) + `,"embedding":[1` + strings.Repeat(",0", recallDimensions-1) + "]}"
}

func FuzzRecallInputRecord(f *testing.F) {
	f.Add([]byte(recallTestRecord("one")))
	f.Add([]byte(`{"key":"one","embedding":[null]}`))
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = readRecallInput(t.Context(), io.MultiReader(strings.NewReader(recallTestHeader()+"\n"), strings.NewReader(string(data))), 1, nil)
	})
}
