package volume

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"

	"github.com/dubi-project/dubi/internal/platform/identity"
)

// These are internal resource kinds, not Engine IDs or deployment knobs.
// The Store's existing durable manifest is the parent catalog. A mandatory
// entry is created together with Store identity, never inferred from a missing
// child's registration. Additional required kinds need an explicit format path.
const ImmutableRecordsResource = "immutable-records"
const ImmutableRecordsDirectory = "records"
const ImmutableRecordsMarker = "DUBI_RECORDS"
const ResourcePending = "pending"
const ResourceActive = "active"

type ResourceIntent struct {
	ResourceID string `json:"resource_id"`
	State      string `json:"state"`
}

func validateResources(resources map[string]ResourceIntent) error {
	if len(resources) != 1 {
		return errors.New("Store resource catalog is missing required entries or contains unknown kinds")
	}
	r, ok := resources[ImmutableRecordsResource]
	if !ok || !validID(r.ResourceID, "") || r.State != ResourcePending && r.State != ResourceActive {
		return errors.New("invalid immutable-record resource creation intent")
	}
	return nil
}

// ResourceBinding is an owned handle to a pre-existing parent intent. Its
// fields cannot be manufactured by a child component. It is invalid after
// Registry.Close or uncertain publication. A child must be durable before
// Activate and must not accept data until activation is durably confirmed.
type ResourceBinding struct {
	registry *Registry
	kind     string
	id       string
}

func (r *Registry) Resource(kind string) (*ResourceBinding, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkCatalogLocked(); err != nil {
		return nil, err
	}
	intent, ok := r.manifest.Resources[kind]
	if !ok {
		return nil, errors.New("resource has no persistent creation intent")
	}
	return &ResourceBinding{registry: r, kind: kind, id: intent.ResourceID}, nil
}
func (b *ResourceBinding) Root() string    { return b.registry.root }
func (b *ResourceBinding) StoreID() string { return b.registry.manifest.StoreID }
func (b *ResourceBinding) ID() string      { return b.id }
func (b *ResourceBinding) Kind() string    { return b.kind }
func (b *ResourceBinding) Current() (ResourceIntent, error) {
	r := b.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	return b.currentLocked()
}
func (b *ResourceBinding) currentLocked() (ResourceIntent, error) {
	r := b.registry
	if err := r.checkCatalogLocked(); err != nil {
		return ResourceIntent{}, err
	}
	intent, ok := r.manifest.Resources[b.kind]
	if !ok || intent.ResourceID != b.id {
		return ResourceIntent{}, errors.New("parent resource identity changed")
	}
	return intent, nil
}
func (b *ResourceBinding) CheckActive() error {
	intent, err := b.Current()
	if err != nil {
		return err
	}
	if intent.State != ResourceActive {
		return errors.New("parent resource creation not yet activated")
	}
	return nil
}
func (b *ResourceBinding) Activate() error {
	r := b.registry
	r.mu.Lock()
	defer r.mu.Unlock()
	intent, err := b.currentLocked()
	if err != nil {
		return err
	}
	if intent.State == ResourceActive {
		return nil
	}
	intent.State = ResourceActive
	r.manifest.Resources[b.kind] = intent
	return r.save()
}

func (r *Registry) checkCatalogLocked() error {
	if r.closed || r.failed != nil {
		return errors.Join(r.failed, errors.New("Store resource catalog requires recovery or is closed"))
	}
	info, err := os.Lstat(r.root)
	if err != nil || !info.IsDir() || !os.SameFile(r.rootInfo, info) {
		return errors.Join(err, errors.New("Store resource catalog directory replaced"))
	}
	id, err := identity.Load(r.root)
	if err != nil {
		return err
	}
	var current manifest
	if err := readJSON(filepath.Join(r.root, manifestName), &current); err != nil {
		return err
	}
	if err := validateManifest(r.root, current, id); err != nil {
		return err
	}
	if !reflect.DeepEqual(current, r.manifest) {
		return errors.New("Store resource catalog changed outside its owner")
	}
	var stored owner
	if err := readJSON(filepath.Join(r.root, storageName), &stored); err != nil {
		return err
	}
	if stored != r.manifest.owner {
		return errors.New("Store resource catalog identity changed")
	}
	return nil
}
