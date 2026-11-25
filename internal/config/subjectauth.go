package config

// SubjectAuthorityConfig holds staged delegated-token trust settings.
type SubjectAuthorityConfig struct {
	Issuer   string
	Audience string
	JWKSFile string
}
