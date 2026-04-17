package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr            string
	ReadTimeout           time.Duration
	WriteTimeout          time.Duration
	IdleTimeout           time.Duration
	FilerEndpoints        []string
	InternalRoot          string
	HTTPTimeout           time.Duration
	TempDir               string
	DownloadCacheDir      string
	DownloadCacheMaxFiles int
	MaxUploadBytes        int64
	PDFFallbackDPI        int
	WorkerID              int
}

func Load() (Config, error) {
	tempDir := getEnv("TEMP_DIR", os.TempDir())

	cfg := Config{
		ListenAddr:            getEnv("LISTEN_ADDR", ":9082"),
		ReadTimeout:           getDurationEnv("READ_TIMEOUT", 30*time.Second),
		WriteTimeout:          getDurationEnv("WRITE_TIMEOUT", 5*time.Minute),
		IdleTimeout:           getDurationEnv("IDLE_TIMEOUT", 60*time.Second),
		InternalRoot:          strings.TrimRight(getEnv("SEAWEEDFS_INTERNAL_ROOT", "/connector/dms"), "/"),
		HTTPTimeout:           getDurationEnv("SEAWEEDFS_HTTP_TIMEOUT", 60*time.Second),
		TempDir:               tempDir,
		DownloadCacheDir:      getEnv("DOWNLOAD_CACHE_DIR", filepath.Join(tempDir, "dms-download-cache")),
		DownloadCacheMaxFiles: getIntEnv("DOWNLOAD_CACHE_MAX_FILES", 128),
		MaxUploadBytes:        getInt64Env("MAX_UPLOAD_BYTES", 64<<20),
		PDFFallbackDPI:        getIntEnv("PDF_RENDER_DPI", 144),
		WorkerID:              getIntEnv("WORKER_ID", 0),
	}

	raw := getEnv("SEAWEEDFS_FILERS", "")
	if raw == "" {
		return cfg, fmt.Errorf("SEAWEEDFS_FILERS must be set")
	}

	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		cfg.FilerEndpoints = append(cfg.FilerEndpoints, strings.TrimRight(item, "/"))
	}

	if len(cfg.FilerEndpoints) == 0 {
		return cfg, fmt.Errorf("SEAWEEDFS_FILERS resolved to zero endpoints")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}

	return fallback
}

func getInt64Env(key string, fallback int64) int64 {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return parsed
		}
	}

	return fallback
}

func getIntEnv(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}

	return fallback
}
