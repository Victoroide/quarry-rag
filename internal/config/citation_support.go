package config

import (
	"fmt"
	"os"

	"github.com/Victoroide/quarry-rag/internal/citationsupport"
)

func (c *Config) loadCitationSupport() error {
	c.CitationSupportTimeout = citationsupport.DefaultTimeout
	c.CitationSupportMaxPairs = citationsupport.DefaultMaxPairs
	if c.FakeMode {
		return nil
	}
	c.CitationSupportModel = os.Getenv("CITATION_SUPPORT_MODEL")
	if c.CitationSupportModel == "" {
		return nil
	}
	c.CitationSupportEndpoint = os.Getenv("CITATION_SUPPORT_ENDPOINT")
	c.CitationSupportToken = os.Getenv("CITATION_SUPPORT_TOKEN")
	c.CitationSupportCalibrationFile = os.Getenv("CITATION_SUPPORT_CALIBRATION_FILE")
	if err := citationsupport.ValidateSettings(c.CitationSupportModel, c.CitationSupportEndpoint, c.CitationSupportToken); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	var err error
	c.CitationSupportTimeout, err = envDurationOrDefault("CITATION_SUPPORT_TIMEOUT", citationsupport.DefaultTimeout)
	if err != nil {
		return err
	}
	if c.CitationSupportTimeout <= 0 || c.CitationSupportTimeout >= c.QueryTimeout {
		return fmt.Errorf("config: CITATION_SUPPORT_TIMEOUT must be positive and below QUERY_TIMEOUT")
	}
	c.CitationSupportMaxPairs, err = envIntOrDefault("CITATION_SUPPORT_MAX_PAIRS", citationsupport.DefaultMaxPairs)
	if err != nil {
		return err
	}
	if c.CitationSupportMaxPairs < 1 || c.CitationSupportMaxPairs > citationsupport.MaxPairs {
		return fmt.Errorf("config: CITATION_SUPPORT_MAX_PAIRS must be in [1, %d]", citationsupport.MaxPairs)
	}
	return nil
}
