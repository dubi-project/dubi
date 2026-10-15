package volume

import (
	"errors"
	"os"
)

// WithVolume pins and validates a registered volume for one storage operation.
// The root must not escape the callback. Operations are serialized with registry
// changes and Close; callers must not re-enter the registry from the callback.
// This is a correctness-first gate, not the final per-volume IO scheduler.
func (r *Registry) WithVolume(id string, write bool, fn func(root *os.Root, storeID string) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.withVolumeLocked(id, write, false, fn)
}

// WithVolumeMaintenance permits an internal, already-authorized maintenance
// pass to mutate a Draining Volume. It never permits ordinary appends: the
// caller must have durably entered Drain first, and the callback is used only
// for sealed-Pack migration/GC and its catalog publication.
func (r *Registry) WithVolumeMaintenance(id string, fn func(root *os.Root, storeID string) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.withVolumeLocked(id, true, true, fn)
}

func (r *Registry) withVolumeLocked(id string, write, maintenance bool, fn func(root *os.Root, storeID string) error) error {
	if r.closed || r.failed != nil {
		return errors.Join(r.failed, errors.New("volume registry is closed or requires recovery"))
	}
	for _, record := range r.manifest.Volumes {
		if record.ID != id {
			continue
		}
		if maintenance {
			if record.State != Active && record.State != Draining {
				return errors.New("volume does not permit maintenance")
			}
		} else if record.State != Active && (write || record.State != Draining) {
			return errors.New("volume does not permit this storage operation")
		}
		if err := r.verify(record); err != nil {
			return err
		}
		if err := r.hold(record); err != nil {
			return err
		}
		root, err := os.OpenRoot(record.Path)
		if err != nil {
			return err
		}
		defer root.Close()
		// Bind the pinned directory to the marker whose lock we actually own.
		held, err := r.markers[id].Stat()
		if err != nil {
			return err
		}
		current, err := root.Lstat(markerName)
		if err != nil {
			return err
		}
		if !current.Mode().IsRegular() || !os.SameFile(held, current) {
			return errors.New("volume changed while acquiring storage access")
		}
		return fn(root, r.manifest.StoreID)
	}
	return errors.New("unknown VolumeID")
}
