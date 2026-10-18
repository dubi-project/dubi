package repository

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

const s3ServiceName = "s3"

// S3Credentials are supplied by the recovery platform after decrypting the
// Repository credential envelope. They are intentionally separate from the
// persisted Repository record.
type S3Credentials struct {
	AccessKey    string `json:"access_key"`
	SecretKey    string `json:"secret_key"`
	SessionToken string `json:"session_token,omitempty"`
}

// SealS3Credentials returns a copy of value with its S3 credentials stored as
// an authenticated encrypted envelope. The plaintext document never becomes
// part of the Repository record or the Root command.
func SealS3Credentials(value Repository, key []byte, keyVersion uint32, credentials S3Credentials) (Repository, error) {
	if err := value.Validate(); err != nil {
		return Repository{}, err
	}
	if !value.IsS3() {
		return Repository{}, errors.New("S3 credentials require an S3 Repository")
	}
	if keyVersion == 0 {
		return Repository{}, errors.New("S3 credential key version is required")
	}
	if err := validateS3Credentials(credentials); err != nil {
		return Repository{}, err
	}
	document, err := json.Marshal(credentials)
	if err != nil {
		return Repository{}, err
	}
	ciphertext, nonce, algorithm, err := EncryptCredentials(key, document)
	if err != nil {
		return Repository{}, err
	}
	value.EncryptedCredentials = ciphertext
	value.CredentialsNonce = nonce
	value.CredentialsAlgorithm = CredentialAlgorithmName(algorithm)
	value.CredentialsKeyVersion = keyVersion
	return value, nil
}

// OpenS3Credentials decrypts the Repository credential envelope. The caller
// owns the returned plaintext and must keep it out of logs and persistent
// Root state.
func OpenS3Credentials(value Repository, key []byte) (S3Credentials, error) {
	if err := value.Validate(); err != nil {
		return S3Credentials{}, err
	}
	if !value.IsS3() {
		return S3Credentials{}, errors.New("S3 credentials require an S3 Repository")
	}
	if len(value.EncryptedCredentials) == 0 {
		return S3Credentials{}, errors.New("S3 Repository has no credential envelope")
	}
	algorithm, err := CredentialAlgorithmID(value.CredentialsAlgorithm)
	if err != nil {
		return S3Credentials{}, err
	}
	document, err := DecryptCredentials(key, value.EncryptedCredentials, value.CredentialsNonce, algorithm)
	if err != nil {
		return S3Credentials{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	var credentials S3Credentials
	if err := decoder.Decode(&credentials); err != nil {
		return S3Credentials{}, errors.New("invalid S3 credential document")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return S3Credentials{}, errors.New("invalid S3 credential document")
	}
	if err := validateS3Credentials(credentials); err != nil {
		return S3Credentials{}, err
	}
	return credentials, nil
}

func validateS3Credentials(value S3Credentials) error {
	if value.AccessKey == "" && value.SecretKey == "" && value.SessionToken == "" {
		return nil
	}
	if value.AccessKey == "" || value.SecretKey == "" || len(value.AccessKey) > 256 || len(value.SecretKey) > 1024 || len(value.SessionToken) > 16<<10 {
		return errors.New("invalid S3 credentials")
	}
	return nil
}

type S3Store struct {
	repository  Repository
	credentials S3Credentials
	client      *http.Client
	endpoint    string
	// namespace separates node-local manifests when several workers use the
	// same S3 Repository. It is deliberately not part of the cluster
	// Repository definition because it is derived from the current Node.
	namespace string
}

var _ ObjectStore = (*S3Store)(nil)

// OpenS3 creates an S3-compatible ObjectStore. The first implementation uses
// the standard S3 REST API and AWS Signature Version 4 when credentials are
// supplied. A custom Endpoint is useful for S3-compatible services and tests.
func OpenS3(value Repository, credentials S3Credentials) (*S3Store, error) {
	return openS3(value, credentials, "")
}

// OpenS3Node opens the node-local view of an S3 Repository. S3 is a shared
// destination, but each backup worker owns its own artifacts and Manifest.
// The node namespace keeps workers from trying to publish different
// manifests under the same epoch. It does not transfer or replicate object
// bytes between nodes.
func OpenS3Node(value Repository, credentials S3Credentials, nodeID string) (*S3Store, error) {
	if !validS3NodeNamespace(nodeID) {
		return nil, errors.New("invalid S3 node namespace")
	}
	return openS3(value, credentials, "nodes/"+nodeID)
}

func openS3(value Repository, credentials S3Credentials, namespace string) (*S3Store, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	if !value.IsS3() {
		return nil, errors.New("S3 store requires an S3 Repository")
	}
	if credentials.SecretKey != "" && credentials.AccessKey == "" {
		return nil, errors.New("S3 access key is required with a secret key")
	}
	endpoint := value.Endpoint
	if endpoint == "" {
		region := value.Region
		if region == "" {
			region = "us-east-1"
		}
		endpoint = "https://s3." + region + ".amazonaws.com"
	}
	return &S3Store{repository: value, credentials: credentials, client: http.DefaultClient, endpoint: endpoint, namespace: namespace}, nil
}

func (s *S3Store) PutObject(ctx context.Context, source io.Reader) (ObjectRef, error) {
	if s == nil || source == nil {
		return ObjectRef{}, errors.New("S3 object store is not ready")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	temporary, err := os.CreateTemp("", "dubi-s3-upload-")
	if err != nil {
		return ObjectRef{}, err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	hash := sha256.New()
	written, err := copyWithContext(ctx, io.MultiWriter(temporary, hash), source)
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return ObjectRef{}, err
	}
	id := hex.EncodeToString(hash.Sum(nil))
	object := ObjectRef{ID: id, Size: written}
	found, err := s.HasObject(id)
	if err != nil {
		return ObjectRef{}, err
	}
	if found {
		if err := s.verifyObject(ctx, object); err != nil {
			return ObjectRef{}, err
		}
		return object, nil
	}
	file, err := os.Open(temporaryName)
	if err != nil {
		return ObjectRef{}, err
	}
	defer file.Close()
	if err := s.request(ctx, http.MethodPut, s.objectURL(id), file, written, map[string]string{"If-None-Match": "*"}); err != nil {
		if !isPreconditionFailed(err) {
			return ObjectRef{}, err
		}
	}
	if err := s.verifyObject(ctx, object); err != nil {
		return ObjectRef{}, err
	}
	return object, nil
}

func (s *S3Store) HasObject(id string) (bool, error) {
	if s == nil || !validObjectID(id) {
		return false, errors.New("invalid S3 object ID")
	}
	response, err := s.do(context.Background(), http.MethodHead, s.objectURL(id), nil, 0, nil)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	response.Body.Close()
	return true, nil
}

func (s *S3Store) OpenObject(id string) (io.ReadCloser, error) {
	if s == nil || !validObjectID(id) {
		return nil, errors.New("invalid S3 object ID")
	}
	response, err := s.do(context.Background(), http.MethodGet, s.objectURL(id), nil, 0, nil)
	if err != nil {
		return nil, err
	}
	return response.Body, nil
}

func (s *S3Store) CommitManifest(ctx context.Context, manifest Manifest) error {
	if s == nil {
		return errors.New("S3 object store is not ready")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	for _, object := range manifest.Objects {
		found, err := s.HasObject(object.ID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("Recovery Point object %s is missing", object.ID)
		}
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if len(encoded) > maxManifestBytes {
		return errors.New("Recovery Point Manifest is too large")
	}
	key := manifestKey(manifest.Epoch)
	if err := s.putImmutable(ctx, key, encoded, "application/json"); err != nil {
		return err
	}
	return s.putImmutable(ctx, key+".complete", []byte("complete\n"), "text/plain")
}

func (s *S3Store) ReadManifest(epoch uint64) (Manifest, error) {
	if s == nil || epoch == 0 {
		return Manifest{}, errors.New("invalid Recovery Point")
	}
	if _, err := s.do(context.Background(), http.MethodHead, s.objectURL(manifestKey(epoch)+".complete"), nil, 0, nil); err != nil {
		if isNotFound(err) {
			return Manifest{}, errors.New("Recovery Point is not complete")
		}
		return Manifest{}, err
	}
	response, err := s.do(context.Background(), http.MethodGet, s.objectURL(manifestKey(epoch)), nil, 0, nil)
	if err != nil {
		return Manifest{}, err
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, maxManifestBytes+1))
	closeErr := response.Body.Close()
	if err != nil {
		return Manifest{}, err
	}
	if closeErr != nil {
		return Manifest{}, closeErr
	}
	if len(encoded) > maxManifestBytes {
		return Manifest{}, errors.New("Recovery Point Manifest is too large")
	}
	var manifest Manifest
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		return Manifest{}, err
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	for _, object := range manifest.Objects {
		if err := s.verifyObject(context.Background(), object); err != nil {
			return Manifest{}, err
		}
	}
	return manifest, nil
}

func (s *S3Store) putImmutable(ctx context.Context, key string, payload []byte, contentType string) error {
	err := s.request(ctx, http.MethodPut, s.objectURL(key), bytes.NewReader(payload), int64(len(payload)), map[string]string{"If-None-Match": "*", "Content-Type": contentType})
	if err == nil || !isPreconditionFailed(err) {
		return err
	}
	response, readErr := s.do(ctx, http.MethodGet, s.objectURL(key), nil, 0, nil)
	if readErr != nil {
		return err
	}
	existing, readErr := io.ReadAll(io.LimitReader(response.Body, maxManifestBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(existing, payload) {
		return errors.Join(err, readErr, closeErr, errors.New("S3 immutable object conflicts with existing content"))
	}
	return nil
}

func (s *S3Store) verifyObject(ctx context.Context, expected ObjectRef) error {
	response, err := s.do(ctx, http.MethodGet, s.objectURL(expected.ID), nil, 0, nil)
	if err != nil {
		return err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(hash, response.Body)
	closeErr := response.Body.Close()
	if copyErr != nil || closeErr != nil {
		return errors.Join(copyErr, closeErr)
	}
	if written != expected.Size || hex.EncodeToString(hash.Sum(nil)) != expected.ID {
		return fmt.Errorf("S3 object %s integrity check failed", expected.ID)
	}
	return nil
}

func (s *S3Store) request(ctx context.Context, method, rawURL string, body io.Reader, size int64, headers map[string]string) error {
	response, err := s.do(ctx, method, rawURL, body, size, headers)
	if response != nil {
		response.Body.Close()
	}
	return err
}

func (s *S3Store) do(ctx context.Context, method, rawURL string, body io.Reader, size int64, headers map[string]string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, err
	}
	if size >= 0 {
		request.ContentLength = size
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	payloadHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if body != nil {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	request.Header.Set("x-amz-content-sha256", payloadHash)
	request.Header.Set("x-amz-date", time.Now().UTC().Format("20060102T150405Z"))
	if s.credentials.SessionToken != "" {
		request.Header.Set("x-amz-security-token", s.credentials.SessionToken)
	}
	if s.credentials.AccessKey != "" {
		s.sign(request, payloadHash)
	}
	client := s.client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		defer response.Body.Close()
		message, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
		return response, &s3HTTPError{StatusCode: response.StatusCode, Message: strings.TrimSpace(string(message))}
	}
	return response, nil
}

type s3HTTPError struct {
	StatusCode int
	Message    string
}

func (e *s3HTTPError) Error() string {
	return fmt.Sprintf("S3 request failed with HTTP %d: %s", e.StatusCode, e.Message)
}

func isNotFound(err error) bool {
	var value *s3HTTPError
	return errors.As(err, &value) && value.StatusCode == http.StatusNotFound
}

func isPreconditionFailed(err error) bool {
	var value *s3HTTPError
	return errors.As(err, &value) && value.StatusCode == http.StatusPreconditionFailed
}

func (s *S3Store) sign(request *http.Request, payloadHash string) {
	region := s.repository.Region
	if region == "" {
		region = "us-east-1"
	}
	amzDate := request.Header.Get("x-amz-date")
	date := amzDate[:8]
	canonicalHeaders, signedHeaders := canonicalHeaders(request)
	canonicalRequest := strings.Join([]string{request.Method, request.URL.EscapedPath(), request.URL.RawQuery, canonicalHeaders, signedHeaders, payloadHash}, "\n")
	scope := date + "/" + region + "/" + s3ServiceName + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hexSHA256([]byte(canonicalRequest))
	key := hmacSHA256([]byte("AWS4"+s.credentials.SecretKey), []byte(date))
	key = hmacSHA256(key, []byte(region))
	key = hmacSHA256(key, []byte(s3ServiceName))
	key = hmacSHA256(key, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256(key, []byte(stringToSign)))
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.credentials.AccessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
}

func canonicalHeaders(request *http.Request) (string, string) {
	values := map[string]string{"host": request.Host}
	for key, list := range request.Header {
		lower := strings.ToLower(key)
		if lower == "authorization" {
			continue
		}
		values[lower] = strings.Join(list, ",")
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var canonical strings.Builder
	for _, key := range keys {
		canonical.WriteString(key)
		canonical.WriteByte(':')
		canonical.WriteString(strings.Join(strings.Fields(values[key]), " "))
		canonical.WriteByte('\n')
	}
	return canonical.String(), strings.Join(keys, ";")
}

func hmacSHA256(key, value []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(value)
	return hash.Sum(nil)
}

func hexSHA256(value []byte) string {
	hash := sha256.Sum256(value)
	return hex.EncodeToString(hash[:])
}

func (s *S3Store) objectURL(id string) string { return s.keyURL(objectKey(id)) }

func (s *S3Store) keyURL(key string) string {
	base, _ := url.Parse(s.endpoint)
	path := strings.TrimRight(base.Path, "/")
	if s.repository.PathStyle {
		path += "/" + s.repository.Bucket
	} else {
		base.Host = s.repository.Bucket + "." + base.Host
	}
	prefix := strings.Trim(s.repository.Prefix, "/")
	if s.namespace != "" {
		if prefix != "" {
			prefix += "/"
		}
		prefix += s.namespace
	}
	if prefix != "" {
		path += "/" + prefix
	}
	base.Path = path + "/" + key
	base.RawPath = ""
	return base.String()
}

func validS3NodeNamespace(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 128 || strings.ContainsAny(value, "/\\\x00") {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func objectKey(id string) string {
	return "objects/" + id[:2] + "/" + id
}

func manifestKey(epoch uint64) string {
	return fmt.Sprintf("manifests/%020d.json", epoch)
}
