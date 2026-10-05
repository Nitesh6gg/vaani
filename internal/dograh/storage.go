package dograh

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/nitesh/vaani/internal/ai/llm"
)

// Storage is Dograh's MinIO bucket, where it keeps call recordings and
// transcripts (api/services/filesystem/minio.py). Configured with Dograh's
// own MINIO_* settings, which live in its environment, not its database.
type Storage struct {
	Endpoint  string // host:port, as Dograh's MINIO_ENDPOINT
	AccessKey string
	SecretKey string
	Bucket    string
	Secure    bool // https
}

// minioRegion is the region MinIO signs for unless configured otherwise.
const minioRegion = "us-east-1"

// Put uploads body as object key (path-style URL, as the MinIO client
// does), over the shared HTTP client.
func (s *Storage) Put(ctx context.Context, key, contentType string, body []byte) error {
	scheme := "http"
	if s.Secure {
		scheme = "https"
	}

	u := url.URL{Scheme: scheme, Host: s.Endpoint, Path: "/" + s.Bucket + "/" + key}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("minio put %s: %w", key, err)
	}

	req.Header.Set("Content-Type", contentType)
	s.sign(req, body, time.Now())

	resp, err := llm.SharedHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("minio put %s: %w", key, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("minio put %s: %s: %s", key, resp.Status, msg)
	}

	return nil
}

// sign adds AWS Signature V4 headers to req, signing content-type, host,
// x-amz-content-sha256 and x-amz-date -- what the MinIO Python client
// (minio/signer.py) signs for the same request.
func (s *Storage) sign(req *http.Request, body []byte, now time.Time) {
	now = now.UTC()
	amzDate := now.Format("20060102T150405Z")
	day := now.Format("20060102")

	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])

	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	const signedHeaders = "content-type;host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := req.Method + "\n" +
		req.URL.EscapedPath() + "\n" +
		req.URL.RawQuery + "\n" +
		"content-type:" + req.Header.Get("Content-Type") + "\n" +
		"host:" + req.URL.Host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n\n" +
		signedHeaders + "\n" +
		payloadHash

	scope := day + "/" + minioRegion + "/s3/aws4_request"
	crHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(crHash[:])

	key := hmacSHA256([]byte("AWS4"+s.SecretKey), day)
	key = hmacSHA256(key, minioRegion)
	key = hmacSHA256(key, "s3")
	key = hmacSHA256(key, "aws4_request")

	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.AccessKey+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+hex.EncodeToString(hmacSHA256(key, stringToSign)))
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))

	return m.Sum(nil)
}
