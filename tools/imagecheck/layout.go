package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	mediaIndex          = "application/vnd.oci.image.index.v1+json"
	mediaDockerIndex    = "application/vnd.docker.distribution.manifest.list.v2+json"
	mediaManifest       = "application/vnd.oci.image.manifest.v1+json"
	mediaDockerManifest = "application/vnd.docker.distribution.manifest.v2+json"

	mediaLayer        = "application/vnd.oci.image.layer.v1.tar"
	mediaLayerGzip    = "application/vnd.oci.image.layer.v1.tar+gzip"
	mediaDockerLayer  = "application/vnd.docker.image.rootfs.diff.tar"
	mediaDockerLayerZ = "application/vnd.docker.image.rootfs.diff.tar.gzip"

	// attestationKey and attestationValue annotate the provenance and SBOM
	// manifests the builder lists next to the images.
	attestationKey   = "vnd.docker.reference.type"
	attestationValue = "attestation-manifest"

	// maxBlobBytes bounds one entry of the archive, which is held in memory.
	maxBlobBytes = 1 << 30
)

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type platform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Variant      string `json:"variant,omitempty"`
}

func (p platform) String() string {
	if p.Variant == "" {
		return p.OS + "/" + p.Architecture
	}
	return p.OS + "/" + p.Architecture + "/" + p.Variant
}

type descriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Annotations map[string]string `json:"annotations"`
	Platform    *platform         `json:"platform"`
}

type index struct {
	Manifests []descriptor `json:"manifests"`
}

type manifest struct {
	Config descriptor   `json:"config"`
	Layers []descriptor `json:"layers"`
}

type imageConfig struct {
	platform
	Config struct {
		User       string   `json:"User"`
		Entrypoint []string `json:"Entrypoint"`
		Cmd        []string `json:"Cmd"`
	} `json:"config"`
}

// image is one manifest of the archive with the platform its index lists.
type image struct {
	platform platform
	manifest manifest
	config   imageConfig
}

// archive holds the documents and blobs of an OCI image layout archive.
type archive struct {
	files map[string][]byte
}

// readFile reads a regular file without following a path outside its directory.
func readFile(name string) ([]byte, error) {
	root, err := os.OpenRoot(filepath.Dir(name))
	if err != nil {
		return nil, err
	}
	data, readErr := root.ReadFile(filepath.Base(name))
	return data, errors.Join(readErr, root.Close())
}

func readArchive(name string) (*archive, error) {
	root, err := os.OpenRoot(filepath.Dir(name))
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", name, err)
	}
	defer func() { _ = root.Close() }()
	file, err := root.Open(filepath.Base(name))
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", name, err)
	}
	defer func() { _ = file.Close() }()

	files := map[string][]byte{}
	reader := tar.NewReader(file)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("cannot unpack %s: %w", name, err)
		}
		member := strings.TrimPrefix(path.Clean(header.Name), "/")
		if header.Typeflag != tar.TypeReg || (member != "oci-layout" && member != "index.json" && !strings.HasPrefix(member, "blobs/sha256/")) {
			continue
		}
		data, err := io.ReadAll(io.LimitReader(reader, maxBlobBytes+1))
		if err != nil {
			return nil, fmt.Errorf("cannot unpack %s: %w", name, err)
		}
		if len(data) > maxBlobBytes {
			return nil, fmt.Errorf("%s: %s exceeds %d bytes", name, member, maxBlobBytes)
		}
		files[member] = data
	}
	if files["oci-layout"] == nil || files["index.json"] == nil {
		return nil, fmt.Errorf("%s is not an image layout", name)
	}
	return &archive{files: files}, nil
}

// blob returns the content stored under a digest after verifying it.
func (a *archive) blob(digest string) ([]byte, error) {
	if !digestPattern.MatchString(digest) {
		return nil, fmt.Errorf("invalid digest: %s", digest)
	}
	data, ok := a.files["blobs/sha256/"+strings.TrimPrefix(digest, "sha256:")]
	if !ok {
		return nil, fmt.Errorf("blob %s is missing", digest)
	}
	sum := sha256.Sum256(data)
	if "sha256:"+hex.EncodeToString(sum[:]) != digest {
		return nil, fmt.Errorf("blob %s is corrupt", digest)
	}
	return data, nil
}

func (a *archive) document(digest string, into any) error {
	data, err := a.blob(digest)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, into); err != nil {
		return fmt.Errorf("document %s: %w", digest, err)
	}
	return nil
}

// images lists every image of the archive, descending into nested indexes and
// skipping the attestation manifests the builder adds.
func (a *archive) images() ([]image, error) {
	var root index
	if err := json.Unmarshal(a.files["index.json"], &root); err != nil {
		return nil, fmt.Errorf("index.json: %w", err)
	}
	var found []image
	if err := a.collect(root, &found); err != nil {
		return nil, err
	}
	if len(found) == 0 {
		return nil, errors.New("the archive holds no image")
	}
	return found, nil
}

func (a *archive) collect(list index, found *[]image) error {
	for _, item := range list.Manifests {
		if item.Annotations[attestationKey] == attestationValue {
			continue
		}
		switch item.MediaType {
		case mediaIndex, mediaDockerIndex:
			var nested index
			if err := a.document(item.Digest, &nested); err != nil {
				return err
			}
			if err := a.collect(nested, found); err != nil {
				return err
			}
		case mediaManifest, mediaDockerManifest:
			var current image
			if err := a.document(item.Digest, &current.manifest); err != nil {
				return err
			}
			if err := a.document(current.manifest.Config.Digest, &current.config); err != nil {
				return err
			}
			current.platform = current.config.platform
			if item.Platform != nil {
				current.platform = *item.Platform
			}
			*found = append(*found, current)
		default:
			return fmt.Errorf("unsupported descriptor type: %s", item.MediaType)
		}
	}
	return nil
}

// filesystem applies the layers of an image in order.
func (a *archive) filesystem(current image) (*rootfs, error) {
	result := newRootfs()
	for _, layer := range current.manifest.Layers {
		data, err := a.blob(layer.Digest)
		if err != nil {
			return nil, err
		}
		var reader io.Reader = bytes.NewReader(data)
		switch layer.MediaType {
		case mediaLayer, mediaDockerLayer:
		case mediaLayerGzip, mediaDockerLayerZ:
			unzipped, err := gzip.NewReader(reader)
			if err != nil {
				return nil, fmt.Errorf("layer %s: %w", layer.Digest, err)
			}
			reader = unzipped
		default:
			return nil, fmt.Errorf("layer %s has unsupported type %s", layer.Digest, layer.MediaType)
		}
		if err := result.applyLayer(reader); err != nil {
			return nil, fmt.Errorf("layer %s: %w", layer.Digest, err)
		}
	}
	return result, nil
}
