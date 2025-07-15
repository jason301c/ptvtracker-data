package models

import "time"

type DatasetMetadata struct {
	ResourceID      string     `json:"id"`
	Name            string     `json:"name"`
	LastModified    CustomTime `json:"last_modified"`
	Format          string     `json:"format"`
	URL             string     `json:"url"`
	PackageID       string     `json:"package_id"`
	DatastoreActive bool       `json:"datastore_active"`
}

type DatasetResponse struct {
	Help    string          `json:"help"`
	Success bool            `json:"success"`
	Result  DatasetMetadata `json:"result"`
}

type VersionInfo struct {
	VersionID    int
	VersionName  string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	IsActive     bool
	SourceURL    string
	Description  string
}