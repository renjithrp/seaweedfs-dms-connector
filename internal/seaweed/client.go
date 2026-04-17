package seaweed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"
)

type Client struct {
	endpoints []string
	http      *http.Client
	next      atomic.Uint64
	cache     *downloadCache
}

type FileMeta struct {
	FileID      string            `json:"file_id"`
	TenantID    string            `json:"tenant_id"`
	StoragePath string            `json:"storage_path"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	UploadedAt  time.Time         `json:"uploaded_at"`
	Tags        map[string]string `json:"tags,omitempty"`
}

type UploadResponse struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func NewClient(endpoints []string, timeout time.Duration, cacheDir string, cacheMaxFiles int) (*Client, error) {
	cache, err := newDownloadCache(cacheDir, cacheMaxFiles)
	if err != nil {
		return nil, err
	}

	return &Client{
		endpoints: endpoints,
		http:      &http.Client{Timeout: timeout},
		cache:     cache,
	}, nil
}

func (c *Client) Health(ctx context.Context) error {
	_, _, err := c.do(ctx, http.MethodGet, "/", nil, nil)
	return err
}

func (c *Client) Exists(ctx context.Context, filerPath string) (bool, error) {
	resp, err := c.rawDo(ctx, http.MethodHead, filerPath, nil, nil)
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, &HTTPError{StatusCode: resp.StatusCode, Body: resp.Status}
	}
	return true, nil
}

func (c *Client) UploadBytes(ctx context.Context, filerPath, fieldName, fileName string, payload []byte) (*UploadResponse, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	part, err := writer.CreateFormFile(fieldName, fileName)
	if err != nil {
		return nil, err
	}

	if _, err := part.Write(payload); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	headers := http.Header{}
	headers.Set("Content-Type", writer.FormDataContentType())

	_, respBody, err := c.do(ctx, http.MethodPost, filerPath, &body, headers)
	if err != nil {
		return nil, err
	}

	var resp UploadResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func (c *Client) UploadFile(ctx context.Context, filerPath, fieldName, fileName, localPath string) (*UploadResponse, error) {
	var lastErr error

	for _, endpoint := range c.endpointOrder() {
		resp, err := c.uploadFileToEndpoint(ctx, endpoint, filerPath, fieldName, fileName, localPath)
		if err != nil {
			lastErr = err
			continue
		}

		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusNotFound || resp.StatusCode >= 500 {
			lastErr = &HTTPError{StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(respBody))}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, &HTTPError{StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(respBody))}
		}

		var uploadResp UploadResponse
		if err := json.Unmarshal(respBody, &uploadResp); err != nil {
			return nil, err
		}

		return &uploadResp, nil
	}

	if lastErr == nil {
		lastErr = errors.New("no filer endpoints available")
	}

	return nil, lastErr
}

func (c *Client) UploadJSON(ctx context.Context, filerPath string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}

	h := http.Header{}
	h.Set("Content-Type", "application/json")
	_, _, err = c.do(ctx, http.MethodPut, filerPath, bytes.NewReader(b), h)
	return err
}

func (c *Client) Delete(ctx context.Context, filerPath string) error {
	_, _, err := c.do(ctx, http.MethodDelete, filerPath, nil, nil)
	return err
}

func (c *Client) uploadFileToEndpoint(ctx context.Context, endpoint, filerPath, fieldName, fileName, localPath string) (*http.Response, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join(u.Path, filerPath)
	if strings.HasSuffix(filerPath, "/") && !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}

	pr, pw := io.Pipe()
	writer := multipart.NewWriter(pw)

	go func() {
		file, err := os.Open(localPath)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		defer file.Close()

		part, err := writer.CreateFormFile(fieldName, fileName)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}

		if _, err := io.Copy(part, file); err != nil {
			_ = pw.CloseWithError(err)
			return
		}

		if err := writer.Close(); err != nil {
			_ = pw.CloseWithError(err)
			return
		}

		_ = pw.Close()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), pr)
	if err != nil {
		_ = pr.Close()
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	return c.http.Do(req)
}

func (c *Client) Download(ctx context.Context, filerPath string) (*http.Response, error) {
	resp, err := c.rawDo(ctx, http.MethodGet, filerPath, nil, nil)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()

		b, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
		return nil, fmt.Errorf("download failed: %s", strings.TrimSpace(string(b)))
	}

	return resp, nil
}

func (c *Client) DownloadToFile(ctx context.Context, filerPath, localPath string) error {
	if c.cache == nil {
		return c.downloadToFile(ctx, filerPath, localPath)
	}

	cachedPath, err := c.cache.GetOrCreate(ctx, filerPath, func(cachePath string) error {
		return c.downloadToFile(ctx, filerPath, cachePath)
	})
	if err != nil {
		return err
	}

	return copyFile(cachedPath, localPath)
}

func (c *Client) downloadToFile(ctx context.Context, filerPath, localPath string) error {
	resp, err := c.Download(ctx, filerPath)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	return err
}

func (c *Client) ReadJSON(ctx context.Context, filerPath string, out any) error {
	_, b, err := c.do(ctx, http.MethodGet, filerPath, nil, nil)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, out)
}

func (c *Client) do(ctx context.Context, method, filerPath string, body io.Reader, headers http.Header) (http.Header, []byte, error) {
	resp, err := c.rawDo(ctx, method, filerPath, body, headers)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	return resp.Header, b, err
}

func (c *Client) rawDo(ctx context.Context, method, filerPath string, body io.Reader, headers http.Header) (*http.Response, error) {
	var payload []byte
	var err error

	if body != nil {
		payload, err = io.ReadAll(body)
		if err != nil {
			return nil, err
		}
	}

	var lastErr error
	for _, endpoint := range c.endpointOrder() {
		u, err := url.Parse(endpoint)
		if err != nil {
			lastErr = err
			continue
		}

		u.Path = path.Join(u.Path, filerPath)
		if strings.HasSuffix(filerPath, "/") && !strings.HasSuffix(u.Path, "/") {
			u.Path += "/"
		}

		var reqBody io.Reader
		if payload != nil {
			reqBody = bytes.NewReader(payload)
		}

		req, err := http.NewRequestWithContext(ctx, method, u.String(), reqBody)
		if err != nil {
			lastErr = err
			continue
		}

		for k, vals := range headers {
			for _, v := range vals {
				req.Header.Add(k, v)
			}
		}

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusNotFound {
			defer resp.Body.Close()

			b, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
			lastErr = &HTTPError{
				StatusCode: resp.StatusCode,
				Body:       strings.TrimSpace(string(b)),
			}
			continue
		}

		if resp.StatusCode >= 500 {
			defer resp.Body.Close()

			b, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
			lastErr = &HTTPError{
				StatusCode: resp.StatusCode,
				Body:       strings.TrimSpace(string(b)),
			}
			continue
		}

		return resp, nil
	}

	if lastErr == nil {
		lastErr = errors.New("no filer endpoints available")
	}

	return nil, lastErr
}

func (c *Client) endpointOrder() []string {
	if len(c.endpoints) <= 1 {
		return c.endpoints
	}

	start := int(c.next.Add(1)-1) % len(c.endpoints)
	out := make([]string, 0, len(c.endpoints))

	for i := 0; i < len(c.endpoints); i++ {
		out = append(out, c.endpoints[(start+i)%len(c.endpoints)])
	}

	return out
}

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http %d: %s", e.StatusCode, e.Body)
}

func copyFile(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	return err
}
