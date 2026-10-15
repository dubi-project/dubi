package volume

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"golang.org/x/sys/unix"
)

type Capacity struct {
	TotalBytes     uint64 `json:"total_bytes"`
	AvailableBytes uint64 `json:"available_bytes"`
}

type Status struct {
	Record
	Health   string   `json:"health"`
	Error    string   `json:"error,omitempty"`
	Device   uint64   `json:"device,omitempty"`
	Capacity Capacity `json:"capacity"`
}

type Report struct {
	StoreID string   `json:"store_id"`
	NodeID  string   `json:"node_id"`
	Volumes []Status `json:"volumes"`
	// Filesystem totals, deduplicated for volumes sharing a device. These
	// are observations, not write admission budgets or allocated quotas.
	Capacity Capacity `json:"capacity"`
}

// Inspect is read-only and can run while the Node owns the registry lock.
// Atomic manifest publication gives it one complete registration snapshot.
func Inspect(root string) (Report, error) {
	root, err := canonicalDirectory(root)
	if err != nil {
		return Report{}, err
	}
	id, err := identity.Load(root)
	if err != nil {
		return Report{}, err
	}
	r := &Registry{root: root}
	if err := readJSON(filepath.Join(root, manifestName), &r.manifest); err != nil {
		return Report{}, err
	}
	if err := validateManifest(root, r.manifest, id); err != nil {
		return Report{}, err
	}
	var saved owner
	if err := readJSON(filepath.Join(root, storageName), &saved); err != nil {
		return Report{}, err
	}
	if saved != r.manifest.owner {
		return Report{}, errors.New("Store identity does not match volume registry")
	}
	return r.status(), nil
}

func (r *Registry) Status() (Report, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.failed != nil {
		return Report{}, errors.Join(r.failed, errors.New("volume registry is closed or requires recovery"))
	}
	return r.status(), nil
}

func (r *Registry) status() Report {
	report := Report{StoreID: r.manifest.StoreID, NodeID: r.manifest.NodeID}
	seen := make(map[uint64]bool)
	for _, record := range r.manifest.Volumes {
		status := Status{Record: record, Health: "healthy"}
		switch record.State {
		case Removed:
			status.Health = "removed"
		case Registered:
			status.Health = "pending"
		default:
			err := r.verify(record)
			if err == nil {
				var stat unix.Statfs_t
				err = unix.Statfs(record.Path, &stat)
				if err == nil {
					status.Device, _, err = directoryIdentity(record.Path)
					if err == nil {
						status.Capacity = Capacity{TotalBytes: uint64(stat.Blocks) * uint64(stat.Bsize),
							AvailableBytes: uint64(stat.Bavail) * uint64(stat.Bsize)}
					}
				}
			}
			if err != nil {
				status.Health, status.Error = "invalid", err.Error()
				if errors.Is(err, os.ErrNotExist) {
					// An existing mountpoint without its registered marker is a
					// different (or empty) Volume, not a missing directory.
					if _, pathErr := os.Lstat(record.Path); errors.Is(pathErr, os.ErrNotExist) {
						status.Health = "missing"
					}
				}
			} else if !seen[status.Device] {
				seen[status.Device] = true
				report.Capacity.TotalBytes += status.Capacity.TotalBytes
				report.Capacity.AvailableBytes += status.Capacity.AvailableBytes
			}
		}
		report.Volumes = append(report.Volumes, status)
	}
	return report
}
