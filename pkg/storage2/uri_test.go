package storage2

import (
	"testing"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		wantScheme  string
		wantBucket  string
		wantPath    string
		wantErr     bool
		wantLocal   bool
		wantCloud   bool
		wantMemory  bool
	}{
		{
			name:       "local path without scheme",
			uri:        "/path/to/dataset",
			wantScheme: "file",
			wantPath:   "/path/to/dataset",
			wantLocal:  true,
		},
		{
			name:       "local path with file scheme",
			uri:        "file:///path/to/dataset",
			wantScheme: "file",
			wantPath:   "/path/to/dataset",
			wantLocal:  true,
		},
		{
			name:       "s3 basic",
			uri:        "s3://my-bucket/path/to/dataset",
			wantScheme: "s3",
			wantBucket: "my-bucket",
			wantPath:   "path/to/dataset",
			wantCloud:  true,
		},
		{
			name:        "s3 with endpoint",
			uri:         "s3://my-bucket/path/to/dataset?endpoint=http://localhost:9000",
			wantScheme:  "s3",
			wantBucket:  "my-bucket",
			wantPath:    "path/to/dataset",
			wantCloud:   true,
		},
		{
			name:        "s3 with region",
			uri:         "s3://my-bucket/path/to/dataset?region=us-west-2",
			wantScheme:  "s3",
			wantBucket:  "my-bucket",
			wantPath:    "path/to/dataset",
			wantCloud:   true,
		},
		{
			name:       "gs basic",
			uri:        "gs://my-bucket/path/to/dataset",
			wantScheme: "gs",
			wantBucket: "my-bucket",
			wantPath:   "path/to/dataset",
			wantCloud:  true,
		},
		{
			name:       "az basic",
			uri:        "az://my-container/path/to/dataset",
			wantScheme: "az",
			wantBucket: "my-container",
			wantPath:   "path/to/dataset",
			wantCloud:  true,
		},
		{
			name:       "mem basic",
			uri:        "mem://test-dataset",
			wantScheme: "mem",
			wantPath:   "test-dataset",
			wantMemory: true,
		},
		{
			name:    "empty URI",
			uri:     "",
			wantErr: true,
		},
		{
			name:    "unsupported scheme",
			uri:     "ftp://server/path",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseURI(tt.uri)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseURI(%q) expected error, got nil", tt.uri)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseURI(%q) unexpected error: %v", tt.uri, err)
				return
			}

			if got.Scheme != tt.wantScheme {
				t.Errorf("Scheme = %q, want %q", got.Scheme, tt.wantScheme)
			}
			if got.Bucket != tt.wantBucket {
				t.Errorf("Bucket = %q, want %q", got.Bucket, tt.wantBucket)
			}
			if got.Path != tt.wantPath {
				t.Errorf("Path = %q, want %q", got.Path, tt.wantPath)
			}
			if got.IsLocal() != tt.wantLocal {
				t.Errorf("IsLocal() = %v, want %v", got.IsLocal(), tt.wantLocal)
			}
			if got.IsCloud() != tt.wantCloud {
				t.Errorf("IsCloud() = %v, want %v", got.IsCloud(), tt.wantCloud)
			}
			if got.IsMemory() != tt.wantMemory {
				t.Errorf("IsMemory() = %v, want %v", got.IsMemory(), tt.wantMemory)
			}
		})
	}
}

func TestParsedURI_String(t *testing.T) {
	tests := []struct {
		name string
		uri  *ParsedURI
		want string
	}{
		{
			name: "file uri",
			uri:  &ParsedURI{Scheme: "file", Path: "/path/to/dataset"},
			want: "file:///path/to/dataset",
		},
		{
			name: "s3 uri",
			uri:  &ParsedURI{Scheme: "s3", Bucket: "bucket", Path: "path/to/dataset"},
			want: "s3://bucket/path/to/dataset",
		},
		{
			name: "s3 uri with endpoint",
			uri:  &ParsedURI{Scheme: "s3", Bucket: "bucket", Path: "path", Endpoint: "http://localhost:9000"},
			want: "s3://bucket/path?endpoint=http://localhost:9000",
		},
		{
			name: "mem uri",
			uri:  &ParsedURI{Scheme: "mem", Path: "dataset"},
			want: "mem://dataset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.uri.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNewStorageConfig(t *testing.T) {
	cfg, err := NewStorageConfig("s3://my-bucket/path/to/dataset")
	if err != nil {
		t.Fatalf("NewStorageConfig error: %v", err)
	}
	if cfg.URI.Scheme != "s3" {
		t.Errorf("Scheme = %q, want %q", cfg.URI.Scheme, "s3")
	}
	if cfg.URI.Bucket != "my-bucket" {
		t.Errorf("Bucket = %q, want %q", cfg.URI.Bucket, "my-bucket")
	}
	if cfg.Options == nil {
		t.Error("Options should not be nil")
	}
}
