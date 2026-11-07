package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func TestEmbeddedReactAssetsAndRouteIsolation(t *testing.T) {
	if err := ValidateConsole(); err != nil {
		t.Fatal(err)
	}
	g := NewGateway(GatewayOptions{})
	read := func(method, target string, headers map[string]string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "https://node"+target, nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		return w
	}
	index := read("GET", "/", nil)
	if index.Code != 200 || !strings.Contains(index.Body.String(), `id="root"`) || index.Header().Get("Cache-Control") != "no-cache" {
		t.Fatal(index.Code, index.Body.String())
	}
	for _, route := range []string{"/ui", "/ui/", "/ui/nodes/example"} {
		w := read("GET", route, nil)
		if w.Code != 200 || w.Body.String() != index.Body.String() {
			t.Fatal("client route", route, w.Code)
		}
	}
	for name := range g.consoleManifest.Files {
		if name == "index.html" {
			continue
		}
		w := read("GET", "/"+name, nil)
		if w.Code != 200 || !strings.Contains(w.Header().Get("Cache-Control"), "immutable") || w.Header().Get("ETag") == "" {
			t.Fatal(name, w.Code, w.Header())
		}
		if w = read("GET", "/"+name, map[string]string{"If-None-Match": w.Header().Get("ETag")}); w.Code != 304 || w.Body.Len() != 0 {
			t.Fatal("asset revalidation", name, w.Code)
		}
		if w = read("HEAD", "/"+name, nil); w.Code != 200 || w.Body.Len() != 0 {
			t.Fatal("HEAD asset", name, w.Code)
		}
	}
	for _, route := range []string{"/assets/missing.js", "/assets/../index.html", "/assets/%2e%2e/index.html", "/manifest.json", "/src/App.tsx", "/api/v1/not-found", "/dubi.raft.GroupService/ReadIndex"} {
		w := read("GET", route, nil)
		if w.Code != 404 || strings.Contains(w.Body.String(), `id="root"`) {
			t.Fatal("route leaked to SPA", route, w.Code)
		}
	}
	if w := read("POST", "/ui/nodes", nil); w.Code != 405 {
		t.Fatal("static mutation", w.Code)
	}
}

func TestConsoleValidationRejectsIncompleteBuild(t *testing.T) {
	fixtures := fstest.MapFS{
		"index.html":             &fstest.MapFile{Data: []byte(`<div id="root"></div><script type="module" src="/assets/app-12345678.js"></script>`)},
		"assets/app-12345678.js": &fstest.MapFile{Data: []byte(`console.log("test fixture")`)},
	}
	manifest := consoleManifest{Format: 1, Files: map[string]string{}}
	for name, file := range fixtures {
		hash := sha256.Sum256(file.Data)
		manifest.Files[name] = hex.EncodeToString(hash[:])
	}
	data, _ := json.Marshal(manifest)
	fixtures["manifest.json"] = &fstest.MapFile{Data: data}
	if _, err := validateConsole(fixtures); err != nil {
		t.Fatal(err)
	}
	fixtures["assets/app-12345678.js"].Data = []byte("changed")
	if _, err := validateConsole(fixtures); err == nil {
		t.Fatal("mixed build accepted")
	}
	delete(fixtures, "assets/app-12345678.js")
	if _, err := validateConsole(fixtures); err == nil {
		t.Fatal("missing asset accepted")
	}
	delete(fixtures, "manifest.json")
	if _, err := validateConsole(fixtures); err == nil {
		t.Fatal("missing manifest accepted")
	}
	if _, err := fs.ReadFile(console, "console/src/App.tsx"); err == nil {
		t.Fatal("source embedded")
	}
}
