// Command imagecheck verifies the runtime image that
// `docker buildx build --output type=oci` writes. For each requested platform
// it applies the image layers like a container runtime, then checks the
// configuration, the executable, the runtime account, the files the server
// needs and the license notices against the build inputs.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const usage = "usage: imagecheck -license <file> -notices <dir> <oci-archive> <os>/<arch>..."

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("imagecheck", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	licensePath := flags.String("license", "", "project license every image must carry")
	noticesDir := flags.String("notices", "", "directory with one expected notice tree per platform, named like linux_amd64")
	if err := flags.Parse(args); err != nil || *licensePath == "" || *noticesDir == "" || flags.NArg() < 2 {
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
	passed, failed, err := verify(flags.Arg(0), flags.Args()[1:], *licensePath, *noticesDir)
	for _, line := range passed {
		_, _ = fmt.Fprintln(stdout, line)
	}
	if err != nil {
		failed = append(failed, err.Error())
	}
	for _, line := range failed {
		_, _ = fmt.Fprintln(stderr, "image-check: "+line)
	}
	if len(failed) > 0 {
		return 1
	}
	return 0
}

// verify checks every wanted platform and returns a line per passing platform
// and a line per problem.
func verify(archivePath string, platforms []string, licensePath, noticesDir string) (passed, failed []string, err error) {
	for _, value := range platforms {
		if !supportedPlatform(value) {
			return nil, nil, fmt.Errorf("unsupported platform: %s", value)
		}
	}
	license, err := readFile(licensePath)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read the project license: %w", err)
	}
	loaded, err := readArchive(archivePath)
	if err != nil {
		return nil, nil, err
	}
	images, err := loaded.images()
	if err != nil {
		return nil, nil, err
	}

	byPlatform := map[string]image{}
	for _, current := range images {
		name := current.platform.String()
		if _, seen := byPlatform[name]; seen {
			failed = append(failed, "duplicate platform "+name)
		}
		byPlatform[name] = current
	}
	for _, name := range platforms {
		if _, found := byPlatform[name]; !found {
			failed = append(failed, "missing platform "+name)
		}
	}
	for name := range byPlatform {
		if !slices.Contains(platforms, name) {
			failed = append(failed, "unexpected platform "+name)
		}
	}
	for _, name := range platforms {
		current, found := byPlatform[name]
		if !found {
			continue
		}
		notices, loadErr := loadInventory(filepath.Join(noticesDir, strings.ReplaceAll(name, "/", "_")))
		if loadErr != nil {
			failed = append(failed, fmt.Sprintf("%s: cannot read the expected notices: %v", name, loadErr))
			continue
		}
		if len(notices) == 0 {
			failed = append(failed, name+": the expected notices are empty")
			continue
		}
		report := &problems{platform: name}
		checkImage(loaded, current, expectation{license: license, notices: notices}, report)
		if len(report.list) > 0 {
			failed = append(failed, report.list...)
			continue
		}
		passed = append(passed, fmt.Sprintf("PASS: %s runs %s %s built for %s with certificates, timezone data and %d notice files", name, requiredUser, serverPath, current.platform.Architecture, len(notices)+1))
	}
	if len(failed) == 0 && len(passed) == 0 {
		return nil, nil, errors.New("no platform was checked")
	}
	return passed, failed, nil
}
