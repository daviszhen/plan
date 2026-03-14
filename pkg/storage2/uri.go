package storage2

import (
	"fmt"
	"net/url"
	"strings"
)

// ParsedURI represents a parsed storage URI
type ParsedURI struct {
	// Scheme is the storage scheme: "file", "s3", "gs", "az", "mem"
	Scheme string
	// Bucket is the bucket/container name (empty for local filesystem)
	Bucket string
	// Path is the object path within the bucket or filesystem path
	Path string
	// Region is the cloud region (optional, for S3)
	Region string
	// Endpoint is a custom endpoint URL (optional, for S3-compatible services)
	Endpoint string
	// Query contains additional query parameters
	Query url.Values
}

// String returns the URI as a string
func (u *ParsedURI) String() string {
	switch u.Scheme {
	case "file", "mem":
		return fmt.Sprintf("%s://%s", u.Scheme, u.Path)
	case "s3", "gs", "az":
		if u.Endpoint != "" {
			return fmt.Sprintf("%s://%s/%s?endpoint=%s", u.Scheme, u.Bucket, u.Path, u.Endpoint)
		}
		return fmt.Sprintf("%s://%s/%s", u.Scheme, u.Bucket, u.Path)
	default:
		return fmt.Sprintf("%s://%s/%s", u.Scheme, u.Bucket, u.Path)
	}
}

// ParseURI parses a storage URI string into its components.
// Supported formats:
//   - file:///path/to/dataset - local filesystem path
//   - s3://bucket/path/to/dataset - S3 bucket
//   - s3://bucket/path?endpoint=http://localhost:9000 - S3-compatible service
//   - gs://bucket/path/to/dataset - Google Cloud Storage
//   - az://container/path/to/dataset - Azure Blob Storage
//   - mem://dataset-name - in-memory storage (for testing)
//   - /path/to/dataset - local filesystem path (default, no scheme)
func ParseURI(uri string) (*ParsedURI, error) {
	// Handle empty URI
	if uri == "" {
		return nil, fmt.Errorf("empty URI")
	}

	// Handle local paths without scheme
	if !strings.Contains(uri, "://") {
		return &ParsedURI{
			Scheme: "file",
			Path:   uri,
		}, nil
	}

	// Parse as URL
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid URI %q: %w", uri, err)
	}

	scheme := strings.ToLower(parsed.Scheme)
	result := &ParsedURI{
		Scheme: scheme,
		Query:  parsed.Query(),
	}

	switch scheme {
	case "file":
		// file:///path/to/dataset
		result.Path = parsed.Path

	case "s3":
		// s3://bucket/path/to/dataset
		result.Bucket = parsed.Host
		result.Path = strings.TrimPrefix(parsed.Path, "/")
		// Extract optional endpoint
		if endpoint := parsed.Query().Get("endpoint"); endpoint != "" {
			result.Endpoint = endpoint
		}
		// Extract optional region
		if region := parsed.Query().Get("region"); region != "" {
			result.Region = region
		}

	case "gs":
		// gs://bucket/path/to/dataset
		result.Bucket = parsed.Host
		result.Path = strings.TrimPrefix(parsed.Path, "/")

	case "az":
		// az://container/path/to/dataset
		result.Bucket = parsed.Host
		result.Path = strings.TrimPrefix(parsed.Path, "/")

	case "mem":
		// mem://dataset-name (in-memory storage for testing)
		result.Path = parsed.Host

	default:
		return nil, fmt.Errorf("unsupported URI scheme %q in %q", scheme, uri)
	}

	return result, nil
}

// IsLocal returns true if the URI points to local filesystem
func (u *ParsedURI) IsLocal() bool {
	return u.Scheme == "file"
}

// IsCloud returns true if the URI points to cloud storage
func (u *ParsedURI) IsCloud() bool {
	return u.Scheme == "s3" || u.Scheme == "gs" || u.Scheme == "az"
}

// IsMemory returns true if the URI points to in-memory storage
func (u *ParsedURI) IsMemory() bool {
	return u.Scheme == "mem"
}

// StorageConfig contains configuration for storage operations
type StorageConfig struct {
	// URI is the parsed storage URI
	URI *ParsedURI
	// Credentials for cloud storage (scheme-specific)
	Credentials interface{}
	// Custom options
	Options map[string]interface{}
}

// S3Credentials contains AWS credentials
type S3Credentials struct {
	// AccessKeyID is the AWS access key ID
	AccessKeyID string
	// SecretAccessKey is the AWS secret access key
	SecretAccessKey string
	// SessionToken is the AWS session token (for temporary credentials)
	SessionToken string
	// Region is the AWS region
	Region string
	// Profile is the AWS profile name (for shared credentials)
	Profile string
	// RoleARN is the IAM role ARN to assume
	RoleARN string
	// ExternalID is the external ID for role assumption
	ExternalID string
}

// GSCredentials contains Google Cloud credentials
type GSCredentials struct {
	// CredentialsFile is the path to the credentials JSON file
	CredentialsFile string
	// CredentialsJSON is the credentials JSON content
	CredentialsJSON []byte
	// ProjectID is the Google Cloud project ID
	ProjectID string
}

// AZCredentials contains Azure credentials
type AZCredentials struct {
	// AccountName is the Azure storage account name
	AccountName string
	// AccountKey is the Azure storage account key
	AccountKey string
	// ConnectionString is the Azure storage connection string
	ConnectionString string
	// SAS token for Azure storage
	SASToken string
}

// NewStorageConfig creates a StorageConfig from a URI string
func NewStorageConfig(uri string) (*StorageConfig, error) {
	parsed, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}
	return &StorageConfig{
		URI:     parsed,
		Options: make(map[string]interface{}),
	}, nil
}
