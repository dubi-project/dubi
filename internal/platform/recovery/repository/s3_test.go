package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestS3StoreBackupAndRestore(t *testing.T) {
	server, objects, requests := newFakeS3(t)
	defer server.Close()
	config := Repository{Name: "primary", Type: TypeS3, Bucket: "dubi", Prefix: "cluster-a", Endpoint: server.URL, PathStyle: true, Region: "test-1"}
	store, err := OpenS3(config, S3Credentials{AccessKey: "access", SecretKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("s3 immutable object")
	object, err := store.PutObject(context.Background(), bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if object.Size != int64(len(content)) {
		t.Fatalf("object = %+v", object)
	}
	manifest := Manifest{Version: 1, Repository: config.Name, ClusterID: "cluster-a", Epoch: 9,
		CreatedAt: time.Unix(9, 0).UTC(), Objects: []ObjectRef{object},
		Artifacts: []Artifact{{Name: "root/state", Kind: "root", NodeID: "node-a", StoreID: "store-a", Object: object}}}
	if err := store.CommitManifest(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.ReadManifest(manifest.Epoch)
	if err != nil || loaded.Repository != manifest.Repository || len(loaded.Artifacts) != 1 {
		t.Fatalf("manifest = %+v, %v", loaded, err)
	}
	reader, err := store.OpenObject(object.ID)
	if err != nil {
		t.Fatal(err)
	}
	decoded, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(decoded, content) {
		t.Fatalf("object = %q, read=%v close=%v", decoded, readErr, closeErr)
	}
	if _, err := store.PutObject(context.Background(), bytes.NewReader(content)); err != nil {
		t.Fatal("idempotent object upload:", err)
	}
	if len(*requests) == 0 || len(*objects) != 3 {
		t.Fatalf("unexpected fake S3 activity: requests=%d objects=%d", len(*requests), len(*objects))
	}
}

func TestS3StoreRejectsTamperedObjectOnManifestRead(t *testing.T) {
	server, objects, _ := newFakeS3(t)
	defer server.Close()
	config := Repository{Name: "primary", Type: TypeS3, Bucket: "dubi", Endpoint: server.URL, PathStyle: true}
	store, err := OpenS3(config, S3Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	object, err := store.PutObject(context.Background(), strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Version: 1, Repository: config.Name, ClusterID: "cluster-a", Epoch: 10,
		CreatedAt: time.Unix(10, 0).UTC(), Objects: []ObjectRef{object}}
	if err := store.CommitManifest(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	(*objects)[objectKey(object.ID)] = []byte("tampered")
	if _, err := store.ReadManifest(manifest.Epoch); err == nil {
		t.Fatal("tampered S3 object was accepted")
	}
}

func TestS3StoreSignsAuthenticatedRequests(t *testing.T) {
	server, _, requests := newFakeS3(t)
	defer server.Close()
	config := Repository{Name: "primary", Type: TypeS3, Bucket: "dubi", Endpoint: server.URL, PathStyle: true, Region: "test-1"}
	store, err := OpenS3(config, S3Credentials{AccessKey: "AKID", SecretKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.HasObject(strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	for _, request := range *requests {
		if strings.HasPrefix(request.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=AKID/") {
			return
		}
	}
	t.Fatal("authenticated S3 request did not contain a SigV4 Authorization header")
}

type fakeS3Request struct {
	Method string
	Path   string
	Header http.Header
}

func newFakeS3(t *testing.T) (*httptest.Server, *map[string][]byte, *[]fakeS3Request) {
	t.Helper()
	var mu sync.Mutex
	objects := make(map[string][]byte)
	requests := make([]fakeS3Request, 0)
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		requests = append(requests, fakeS3Request{Method: request.Method, Path: request.URL.Path, Header: request.Header.Clone()})
		mu.Unlock()
		key := strings.TrimPrefix(request.URL.Path, "/dubi/")
		if request.Method == http.MethodHead {
			mu.Lock()
			payload, found := objects[key]
			mu.Unlock()
			if !found {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			writer.Header().Set("Content-Length", stringSize(len(payload)))
			writer.WriteHeader(http.StatusOK)
			return
		}
		if request.Method == http.MethodGet {
			mu.Lock()
			payload, found := objects[key]
			mu.Unlock()
			if !found {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = writer.Write(payload)
			return
		}
		if request.Method == http.MethodPut {
			if request.Header.Get("If-None-Match") == "*" {
				mu.Lock()
				_, found := objects[key]
				mu.Unlock()
				if found {
					writer.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			objects[key] = payload
			mu.Unlock()
			writer.WriteHeader(http.StatusOK)
			return
		}
		writer.WriteHeader(http.StatusMethodNotAllowed)
	})
	server := httptest.NewServer(handler)
	return server, &objects, &requests
}

func stringSize(value int) string {
	return fmt.Sprintf("%d", value)
}
