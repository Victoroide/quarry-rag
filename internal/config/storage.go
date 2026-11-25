package config

import (
	"errors"
	"os"
	"strings"
)

// LoadSourceUploadMaintenance loads the database and S3 settings needed to
// reclaim source uploads independently of the serving configuration.
func LoadSourceUploadMaintenance() (*Config, error) {
	cfg := &Config{}
	if err := cfg.loadStorageEnv(); err != nil {
		return nil, err
	}
	fakeMode, err := envBoolOrDefault("GO_RAG_FAKE_MODE", false)
	if err != nil {
		return nil, err
	}
	if fakeMode || strings.TrimSpace(cfg.DatabaseURL) == "" || strings.TrimSpace(cfg.S3Bucket) == "" || strings.TrimSpace(cfg.AWSRegion) == "" {
		return nil, errors.New("source upload reclamation requires DATABASE_URL, S3_BUCKET and AWS_REGION outside offline mode")
	}
	if err := cfg.validateAWS(); err != nil {
		return nil, err
	}
	if err := cfg.validateS3Owner(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) loadStorageEnv() error {
	c.DatabaseURL = os.Getenv("DATABASE_URL")
	c.S3Bucket = os.Getenv("S3_BUCKET")
	c.S3ExpectedBucketOwner = os.Getenv("S3_EXPECTED_BUCKET_OWNER")
	c.AWSRegion = os.Getenv("AWS_REGION")
	c.AWSAccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	c.AWSSecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	c.AWSSessionToken = os.Getenv("AWS_SESSION_TOKEN")
	attempts, err := envIntOrDefault("AWS_MAX_ATTEMPTS", 4)
	c.AWSMaxAttempts = attempts
	return err
}

func (c *Config) validateAWS() error {
	if (c.AWSAccessKeyID == "") != (c.AWSSecretKey == "") {
		return errors.New("config: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be both set or both empty")
	}
	if c.AWSMaxAttempts < 1 || c.AWSMaxAttempts > 10 {
		return errors.New("config: AWS_MAX_ATTEMPTS must be between 1 and 10")
	}
	return nil
}

func (c *Config) validateS3Owner() error {
	owner := c.S3ExpectedBucketOwner
	if owner != "" && (len(owner) != 12 || strings.IndexFunc(owner, func(r rune) bool { return r < '0' || r > '9' }) >= 0) {
		return errors.New("config: S3_EXPECTED_BUCKET_OWNER must contain exactly 12 digits")
	}
	return nil
}
