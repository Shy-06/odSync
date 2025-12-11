package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type Config struct {
	Port       string
	StorageDir string
	UpstreamURL string
	CacheSize  int64 // MB
}

type FileDownloader struct {
	locks sync.Map // path -> *sync.Mutex
}

var config Config
var downloader = &FileDownloader{}

func main() {
	flag.StringVar(&config.Port, "port", "8080", "Server port")
	flag.StringVar(&config.StorageDir, "storage", "./storage", "Storage directory")
	flag.StringVar(&config.UpstreamURL, "upstream", "https://mirrors.tuna.tsinghua.edu.cn", "Upstream mirror URL")
	flag.Int64Var(&config.CacheSize, "cache-size", 10240, "Cache size in MB")
	flag.Parse()

	if err := os.MkdirAll(config.StorageDir, 0755); err != nil {
		log.Fatalf("Failed to create storage directory: %v", err)
	}

	r := gin.Default()
	
	// API routes with prefix to avoid conflicts
	api := r.Group("/api")
	{
		api.GET("/health", healthCheck)
		api.GET("/stats", getStats)
	}
	
	// All other routes go to file proxy
	r.NoRoute(proxyHandler)
	
	log.Printf("Starting odSync on port %s", config.Port)
	log.Printf("Storage: %s, Upstream: %s", config.StorageDir, config.UpstreamURL)
	
	if err := r.Run(":" + config.Port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
		"version": "1.0.0",
	})
}

func getStats(c *gin.Context) {
	var totalSize int64
	var fileCount int
	
	filepath.Walk(config.StorageDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalSize += info.Size()
			fileCount++
		}
		return nil
	})
	
	c.JSON(http.StatusOK, gin.H{
		"cached_files": fileCount,
		"cache_size_mb": totalSize / 1024 / 1024,
		"cache_limit_mb": config.CacheSize,
		"storage_dir": config.StorageDir,
		"upstream": config.UpstreamURL,
	})
}

func (fd *FileDownloader) getFileLock(path string) *sync.Mutex {
	actual, _ := fd.locks.LoadOrStore(path, &sync.Mutex{})
	return actual.(*sync.Mutex)
}

func (fd *FileDownloader) cleanupLock(path string) {
	fd.locks.Delete(path)
}

func proxyHandler(c *gin.Context) {
	requestPath := c.Request.URL.Path
	localPath := filepath.Join(config.StorageDir, requestPath)
	
	// Check if complete file exists locally
	if isFileComplete(localPath) {
		log.Printf("Cache HIT: %s", requestPath)
		c.File(localPath)
		return
	}
	
	log.Printf("Cache MISS: %s, fetching from upstream", requestPath)
	
	// Acquire per-file lock to prevent concurrent downloads
	fileLock := downloader.getFileLock(localPath)
	fileLock.Lock()
	defer fileLock.Unlock()
	defer downloader.cleanupLock(localPath)
	
	// Double-check after acquiring lock
	if isFileComplete(localPath) {
		log.Printf("Cache HIT (after lock): %s", requestPath)
		c.File(localPath)
		return
	}
	
	// Download with atomic write
	if err := downloadFile(localPath, requestPath); err != nil {
		log.Printf("Download failed: %v", err)
		if err == errUpstreamNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "File not found on upstream"})
		} else {
			c.JSON(http.StatusBadGateway, gin.H{"error": "Failed to fetch from upstream"})
		}
		return
	}
	
	// Verify and serve
	if !isFileComplete(localPath) {
		log.Printf("File verification failed: %s", requestPath)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "File verification failed"})
		return
	}
	
	log.Printf("Cache STORED: %s", requestPath)
	c.File(localPath)
}

var errUpstreamNotFound = fmt.Errorf("upstream file not found")

func downloadFile(localPath, requestPath string) error {
	upstreamURL := config.UpstreamURL + requestPath
	
	// Fetch from upstream
	resp, err := http.Get(upstreamURL)
	if err != nil {
		return fmt.Errorf("fetch error: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == http.StatusNotFound {
		return errUpstreamNotFound
	}
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upstream returned status %d", resp.StatusCode)
	}
	
	// Create directory
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("mkdir error: %w", err)
	}
	
	// Atomic write: download to temp file first
	tempPath := localPath + ".tmp." + generateTempSuffix()
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("create temp file error: %w", err)
	}
	
	// Ensure cleanup on error
	success := false
	defer func() {
		tempFile.Close()
		if !success {
			os.Remove(tempPath)
		}
	}()
	
	// Download with integrity check
	hasher := sha256.New()
	writer := io.MultiWriter(tempFile, hasher)
	
	written, err := io.Copy(writer, resp.Body)
	if err != nil {
		return fmt.Errorf("download error: %w", err)
	}
	
	// Verify content length if provided
	if resp.ContentLength > 0 && written != resp.ContentLength {
		return fmt.Errorf("incomplete download: got %d bytes, expected %d", written, resp.ContentLength)
	}
	
	// Sync to disk before rename
	if err := tempFile.Sync(); err != nil {
		return fmt.Errorf("sync error: %w", err)
	}
	
	tempFile.Close()
	
	// Save checksum for integrity verification
	checksumPath := localPath + ".sha256"
	checksum := fmt.Sprintf("%x", hasher.Sum(nil))
	if err := os.WriteFile(checksumPath, []byte(checksum), 0644); err != nil {
		log.Printf("Warning: failed to save checksum for %s: %v", requestPath, err)
	}
	
	// Atomic rename (POSIX guarantees atomicity)
	if err := os.Rename(tempPath, localPath); err != nil {
		return fmt.Errorf("atomic rename error: %w", err)
	}
	
	success = true
	log.Printf("Downloaded: %s (%d bytes, sha256: %s)", requestPath, written, checksum[:16])
	return nil
}

func isFileComplete(path string) bool {
	// Check if file exists and is not empty
	fileInfo, err := os.Stat(path)
	if err != nil || fileInfo.IsDir() || fileInfo.Size() == 0 {
		return false
	}
	
	// Check if temp files exist (indicates incomplete download)
	pattern := path + ".tmp.*"
	matches, _ := filepath.Glob(pattern)
	if len(matches) > 0 {
		return false
	}
	
	// Verify checksum if available
	checksumPath := path + ".sha256"
	if checksumData, err := os.ReadFile(checksumPath); err == nil {
		expectedChecksum := string(checksumData)
		if actualChecksum := calculateChecksum(path); actualChecksum != expectedChecksum {
			log.Printf("Checksum mismatch for %s, removing corrupted file", path)
			os.Remove(path)
			os.Remove(checksumPath)
			return false
		}
	}
	
	return true
}

func calculateChecksum(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()
	
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return ""
	}
	
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

func generateTempSuffix() string {
	return fmt.Sprintf("%d.%d", os.Getpid(), time.Now().UnixNano())
}
