// Package api is a hand-rolled client for the Ingressive HTTP API.
//
// The package is deliberately self-contained: it must not import any other
// package in this repo (bootstrap, config, cmd/...). The intent is that this
// package can later be extracted as a public Go module
// (github.com/ingressive-cloud/api-go-client) without restructuring.
//
// sigv4.go implements the AWS Signature Version 4 algorithm used by Bifrost's
// "BFAK" access keys. Lifted from bifrost/api/auth.go — kept here so the
// controller does not depend on aws-sdk-go-v2.
package api

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	awsRegion  = "global"
	awsService = "api"
)

// signRequest signs an HTTP request using AWS Signature Version 4.
// keyID is the BFAK... access key ID; secret is the raw base64 secret key.
// After this returns, req will have Authorization and x-amz-date headers set.
func signRequest(req *http.Request, keyID, secret string) error {
	return signAt(req, keyID, secret, time.Now().UTC())
}

func signAt(req *http.Request, keyID, secret string, now time.Time) error {
	dateOnly := now.Format("20060102")
	dateTime := now.Format("20060102T150405Z")
	req.Header.Set("x-amz-date", dateTime)

	host := req.Host
	if host == "" && req.URL != nil {
		host = req.URL.Host
	}

	// Read & restore the body so we can hash it and still let net/http send it.
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return fmt.Errorf("read body: %w", err)
		}
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}
	bodyHash := sha256Hex(bodyBytes)

	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	canonicalHeaders := "host:" + host + "\nx-amz-date:" + dateTime + "\n"
	signedHeaders := "host;x-amz-date"

	canonicalReq := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQueryString(req.URL.Query()),
		canonicalHeaders,
		signedHeaders,
		bodyHash,
	}, "\n")

	credentialScope := dateOnly + "/" + awsRegion + "/" + awsService + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + dateTime + "\n" + credentialScope + "\n" + sha256Hex([]byte(canonicalReq))
	sig := hex.EncodeToString(hmacSHA256(signingKey(secret, dateOnly), []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		keyID, credentialScope, signedHeaders, sig,
	))
	return nil
}

func signingKey(secret, date string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(awsRegion))
	kService := hmacSHA256(kRegion, []byte(awsService))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func canonicalQueryString(q url.Values) string {
	if len(q) == 0 {
		return ""
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		ek := url.QueryEscape(k)
		for _, v := range vals {
			parts = append(parts, ek+"="+url.QueryEscape(v))
		}
	}
	return strings.Join(parts, "&")
}
