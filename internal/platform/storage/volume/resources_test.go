package volume

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func resource(t *testing.T, r *Registry) *ResourceBinding {
	t.Helper()
	b, err := r.Resource(ImmutableRecordsResource)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestStoreResourceIntentLifecycle(t *testing.T) {
	root := testStore(t)
	r, err := Initialize(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b := resource(t, r)
	current, err := b.Current()
	if err != nil || current.State != ResourcePending || b.ID() != current.ResourceID || b.StoreID() != status(t, r).StoreID || b.Root() != root || b.Kind() != ImmutableRecordsResource {
		t.Fatal(current, err)
	}
	if err := b.CheckActive(); err == nil {
		t.Fatal("pending accepted")
	}
	if _, err := r.Resource("file"); err == nil {
		t.Fatal("invented child intent")
	}
	if err := b.Activate(); err != nil {
		t.Fatal(err)
	}
	if err := b.Activate(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Add(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := b.CheckActive(); err != nil {
		t.Fatal("normal catalog update invalidated binding", err)
	}
	closeRegistry(t, r)
	if _, err := b.Current(); err == nil {
		t.Fatal("closed binding usable")
	}
	r, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	next := resource(t, r)
	if next.ID() != b.ID() {
		t.Fatal("resource identity regenerated")
	}
	if err := next.CheckActive(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreResourcePublicationUncertainty(t *testing.T) {
	for _, persist := range []bool{false, true} {
		t.Run(fmt.Sprint(persist), func(t *testing.T) {
			root := testStore(t)
			r, err := Initialize(root, nil)
			if err != nil {
				t.Fatal(err)
			}
			b := resource(t, r)
			injected := errors.New("uncertain catalog publication")
			r.publish = func(m manifest) error {
				if persist {
					if err := writeJSON(filepath.Join(root, manifestName), ".resource-test-", m, false); err != nil {
						return err
					}
				}
				return injected
			}
			if err := b.Activate(); !errors.Is(err, injected) {
				t.Fatal(err)
			}
			if _, err := b.Current(); !errors.Is(err, injected) {
				t.Fatal("uncertain catalog returned intent", err)
			}
			if _, err := r.Resource(ImmutableRecordsResource); !errors.Is(err, injected) {
				t.Fatal(err)
			}
			closeRegistry(t, r)
			r, err = Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			next := resource(t, r)
			got, err := next.Current()
			if err != nil {
				t.Fatal(err)
			}
			want := ResourcePending
			if persist {
				want = ResourceActive
			}
			if got.ResourceID != b.ID() || got.State != want {
				t.Fatal(got, want)
			}
			if err := next.Activate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStoreResourceCatalogLossAndCorruption(t *testing.T) {
	for _, kind := range []string{"manifest", "owner", "node-identity", "entry", "resource-id", "phase", "extra", "version", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			root := testStore(t)
			r, err := Initialize(root, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			b := resource(t, r)
			path := filepath.Join(root, manifestName)
			switch kind {
			case "manifest", "owner", "node-identity", "symlink":
				if kind == "owner" {
					path = filepath.Join(root, storageName)
				}
				if kind == "node-identity" {
					path = filepath.Join(root, "DUBI_NODE_IDENTITY")
				}
				if err := os.Rename(path, path+".lost"); err != nil {
					t.Fatal(err)
				}
				if kind == "symlink" {
					if err := os.Symlink(path+".lost", path); err != nil {
						t.Fatal(err)
					}
				}
			default:
				var m manifest
				if err := readJSON(path, &m); err != nil {
					t.Fatal(err)
				}
				v := m.Resources[ImmutableRecordsResource]
				switch kind {
				case "entry":
					m.Resources = nil
				case "resource-id":
					v.ResourceID = "ffffffffffffffffffffffffffffffff"
					m.Resources[ImmutableRecordsResource] = v
				case "phase":
					v.State = ResourceActive
					m.Resources[ImmutableRecordsResource] = v
				case "extra":
					m.Resources["file"] = v
				case "version":
					m.Version = 1
				}
				if err := writeJSON(path, ".resource-test-", m, false); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := b.Current(); err == nil {
				t.Fatal("catalog mutation accepted")
			}
			if err := b.Activate(); err == nil {
				t.Fatal("activation overwrote lost/changed catalog")
			}
			if _, err := r.Resource(ImmutableRecordsResource); err == nil {
				t.Fatal("new binding bypassed catalog check")
			}
			if _, err := r.Add(t.TempDir()); err == nil {
				t.Fatal("volume addition overwrote lost/changed catalog")
			}
		})
	}
}

func TestStoreResourceMissingOwnerAndParentCannotBeReinitialized(t *testing.T) {
	for _, kind := range []string{"owner", "both-with-marker", "both-with-directory"} {
		t.Run(kind, func(t *testing.T) {
			root := testStore(t)
			r, err := Initialize(root, nil)
			if err != nil {
				t.Fatal(err)
			}
			closeRegistry(t, r)
			path := filepath.Join(root, storageName)
			if err := os.Rename(path, path+".lost"); err != nil {
				t.Fatal(err)
			}
			if kind != "owner" {
				path := filepath.Join(root, manifestName)
				if err := os.Rename(path, path+".lost"); err != nil {
					t.Fatal(err)
				}
				if kind == "both-with-marker" {
					if err := os.WriteFile(filepath.Join(root, ImmutableRecordsMarker), []byte("retained"), 0600); err != nil {
						t.Fatal(err)
					}
				} else if err := os.Mkdir(filepath.Join(root, ImmutableRecordsDirectory), 0700); err != nil {
					t.Fatal(err)
				}
			}
			var paths []string
			if kind != "owner" {
				paths = []string{t.TempDir()}
			}
			if next, err := Initialize(root, paths); err == nil {
				next.Close()
				t.Fatal("new identity installed over lost catalog")
			}
			if _, err := os.Stat(filepath.Join(root, storageName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("identity marker recreated", err)
			}
		})
	}
}

func TestStoreResourceMissingIntentAndLegacyFormatNeverMigrated(t *testing.T) {
	for _, kind := range []string{"legacy", "missing", "unknown", "wrong-id", "wrong-state"} {
		t.Run(kind, func(t *testing.T) {
			root := testStore(t)
			r, err := Initialize(root, nil)
			if err != nil {
				t.Fatal(err)
			}
			closeRegistry(t, r)
			path := filepath.Join(root, manifestName)
			var m manifest
			if err := readJSON(path, &m); err != nil {
				t.Fatal(err)
			}
			v := m.Resources[ImmutableRecordsResource]
			switch kind {
			case "legacy":
				m.Version = 1
				m.Resources = nil
			case "missing":
				m.Resources = map[string]ResourceIntent{}
			case "unknown":
				m.Resources["file"] = v
			case "wrong-id":
				v.ResourceID = "bad"
				m.Resources[ImmutableRecordsResource] = v
			case "wrong-state":
				v.State = "done"
				m.Resources[ImmutableRecordsResource] = v
			}
			if err := writeJSON(path, ".resource-test-", m, false); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, init := range []bool{false, true} {
				var opened *Registry
				if init {
					opened, err = Initialize(root, nil)
				} else {
					opened, err = Open(root)
				}
				if err == nil {
					opened.Close()
					t.Fatal("invalid catalog accepted")
				}
			}
			if _, err := Inspect(root); err == nil {
				t.Fatal("invalid catalog inspected as healthy")
			}
			after, err := os.ReadFile(path)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("catalog automatically changed", err)
			}
		})
	}
}

func TestStoreResourceReturnedIntentIsOwned(t *testing.T) {
	r, err := Initialize(testStore(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b := resource(t, r)
	before, err := b.Current()
	if err != nil {
		t.Fatal(err)
	}
	copy := before
	copy.ResourceID = "bad"
	copy.State = ResourceActive
	encoded, err := json.Marshal(copy)
	if err != nil || len(encoded) == 0 {
		t.Fatal(err)
	}
	after, err := b.Current()
	if err != nil || after != before {
		t.Fatal("returned intent aliases catalog", err)
	}
}
