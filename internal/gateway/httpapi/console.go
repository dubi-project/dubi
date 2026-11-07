package httpapi

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"
)

// console is a generated, versioned snapshot from the separate webui repository.
// Run scripts/build.sh to rebuild React and embed it into the executable.
//
//go:embed console
var console embed.FS

type consoleManifest struct {
	Format int               `json:"format"`
	Files  map[string]string `json:"files"`
}

var consoleDependency = regexp.MustCompile(`(?:src|href)="([^"]+)"`)
var hashedAsset = regexp.MustCompile(`-[A-Za-z0-9_-]{8,}\.[a-z0-9]+$`)

func validateConsole(files fs.FS) (consoleManifest, error) {
	var manifest consoleManifest
	data, err := fs.ReadFile(files, "manifest.json")
	if err != nil || json.Unmarshal(data, &manifest) != nil || manifest.Format != 1 || len(manifest.Files) < 2 || manifest.Files["index.html"] == "" {
		return manifest, errors.New("embedded console manifest is missing or invalid")
	}
	for name, want := range manifest.Files {
		if !fs.ValidPath(name) || (name != "index.html" && !strings.HasPrefix(name, "assets/")) || strings.HasSuffix(name, ".map") {
			return manifest, errors.New("embedded console contains an invalid asset path")
		}
		data, err := fs.ReadFile(files, name)
		if err != nil {
			return manifest, errors.New("embedded console asset is missing")
		}
		digest := sha256.Sum256(data)
		if len(data) == 0 || hex.EncodeToString(digest[:]) != want {
			return manifest, errors.New("embedded console asset checksum mismatch")
		}
	}
	html, err := fs.ReadFile(files, "index.html")
	if err != nil {
		return manifest, errors.New("embedded console index is missing")
	}
	script := false
	for _, match := range consoleDependency.FindAllSubmatch(html, -1) {
		name := string(match[1])
		if !strings.HasPrefix(name, "/assets/") || manifest.Files[strings.TrimPrefix(name, "/")] == "" {
			return manifest, errors.New("embedded console entry references an unbundled dependency")
		}
		script = script || strings.HasSuffix(name, ".js")
	}
	if !script {
		return manifest, errors.New("embedded console entry has no JavaScript bundle")
	}
	return manifest, nil
}

// ValidateConsole makes a missing or mixed build fail startup, rather than
// starting a public listener that can only display a blank page.
func ValidateConsole() error {
	files, err := fs.Sub(console, "console")
	if err != nil {
		return err
	}
	_, err = validateConsole(files)
	return err
}
func isConsoleRequest(r *http.Request) bool {
	return r.URL.Path == "/" || r.URL.Path == "/index.html" || r.URL.Path == "/ui" || strings.HasPrefix(r.URL.Path, "/ui/") || strings.HasPrefix(r.URL.Path, "/assets/")
}
func (g *Gateway) static(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		gatewayError(w, 405, "method_not_allowed", "unsupported method")
		return
	}
	if g.consoleErr != nil {
		gatewayError(w, 503, "unavailable", "console assets unavailable")
		return
	}
	if (path.Clean(r.URL.Path) != r.URL.Path && path.Clean(r.URL.Path)+"/" != r.URL.Path) || strings.Contains(r.URL.Path, "\\") {
		http.NotFound(w, r)
		return
	}
	name := "index.html"
	if strings.HasPrefix(r.URL.Path, "/assets/") {
		name = strings.TrimPrefix(r.URL.Path, "/")
	}
	digest, ok := g.consoleManifest.Files[name]
	if !ok {
		http.NotFound(w, r)
		return
	}
	data, err := console.ReadFile("console/" + name)
	if err != nil {
		gatewayError(w, 503, "unavailable", "console asset unavailable")
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	if name != "index.html" && hashedAsset.MatchString(name) {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	}
	kind := mime.TypeByExtension(path.Ext(name))
	if kind == "" {
		kind = "application/octet-stream"
	}
	w.Header().Set("Content-Type", kind)
	w.Header().Set("ETag", `"`+digest+`"`)
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
}
