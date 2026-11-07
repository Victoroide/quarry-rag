package main

import (
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/Victoroide/quarry-rag/client"
)

type config struct {
	baseURL   string
	token     string
	timeout   time.Duration
	fileRoots []string
	readOnly  bool
}

func loadConfig(getenv func(string) string) (config, error) {
	cfg := config{baseURL: getenv("QUARRY_URL"), token: strings.TrimSpace(getenv("QUARRY_SERVICE_TOKEN")), timeout: 90 * time.Second}
	if strings.ContainsFunc(cfg.token, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return config{}, errors.New("QUARRY_SERVICE_TOKEN must not contain whitespace or control characters")
	}
	if cfg.baseURL == "" {
		return config{}, errors.New("QUARRY_URL is required")
	}
	if !strings.Contains(cfg.baseURL, "://") {
		host, port, err := net.SplitHostPort(cfg.baseURL)
		if err != nil || host == "" || port == "" {
			return config{}, errors.New("QUARRY_URL must be an HTTP address or host:port")
		}
		cfg.baseURL = (&url.URL{Scheme: "http", Host: cfg.baseURL}).String()
	}
	if _, err := client.New(cfg.baseURL); err != nil {
		return config{}, errors.New("QUARRY_URL must be an absolute HTTP or HTTPS address without credentials, query or fragment")
	}
	if value := getenv("QUARRY_TIMEOUT"); value != "" {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout < time.Second || timeout > 10*time.Minute {
			return config{}, errors.New("QUARRY_TIMEOUT must be between 1s and 10m")
		}
		cfg.timeout = timeout
	}
	if value := getenv("QUARRY_MCP_READ_ONLY"); value != "" {
		readOnly, err := strconv.ParseBool(value)
		if err != nil {
			return config{}, errors.New("QUARRY_MCP_READ_ONLY must be a boolean")
		}
		cfg.readOnly = readOnly
	}
	for _, root := range filepath.SplitList(getenv("QUARRY_MCP_FILE_ROOTS")) {
		if root == "" {
			return config{}, errors.New("QUARRY_MCP_FILE_ROOTS must not contain empty directories")
		}
		absolute, err := filepath.Abs(root)
		if err != nil {
			return config{}, errors.New("QUARRY_MCP_FILE_ROOTS contains an invalid directory")
		}
		resolved, err := filepath.EvalSymlinks(absolute)
		if err != nil {
			return config{}, errors.New("QUARRY_MCP_FILE_ROOTS contains an unavailable directory")
		}
		info, err := os.Stat(resolved)
		if err != nil || !info.IsDir() {
			return config{}, errors.New("QUARRY_MCP_FILE_ROOTS must contain directories")
		}
		cfg.fileRoots = append(cfg.fileRoots, resolved)
	}
	return cfg, nil
}
