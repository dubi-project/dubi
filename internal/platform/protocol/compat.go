// Package protocol centralizes the compatibility boundary for DUBI control
// messages and durable local formats. A node never silently opens a format it
// does not understand; callers must negotiate an overlapping protocol range
// before exchanging state-changing messages.
package protocol

import "errors"

const (
	CurrentVersion = 1
	MinimumVersion = 1

	CapabilityStateFormatVersion = 7
	PackContainerFormatVersion   = 2
	PackCatalogFormatVersion     = 2
	RecordIndexFormatVersion     = 1
)

type Advertisement struct {
	ProtocolMin int
	ProtocolMax int
	StateFormat int
	PackFormat  int
	RecordIndex int
}

func CurrentAdvertisement() Advertisement {
	return Advertisement{ProtocolMin: MinimumVersion, ProtocolMax: CurrentVersion,
		StateFormat: CapabilityStateFormatVersion, PackFormat: PackContainerFormatVersion, RecordIndex: RecordIndexFormatVersion}
}

func (a Advertisement) Validate() error {
	if a.ProtocolMin < 1 || a.ProtocolMax < a.ProtocolMin || a.ProtocolMax > CurrentVersion ||
		a.StateFormat != CapabilityStateFormatVersion || a.PackFormat != PackContainerFormatVersion || a.RecordIndex != RecordIndexFormatVersion {
		return errors.New("unsupported DUBI protocol or durable format")
	}
	return nil
}

type Agreement struct {
	Version     int
	StateFormat int
	PackFormat  int
	RecordIndex int
}

func Negotiate(local, remote Advertisement) (Agreement, error) {
	if err := local.Validate(); err != nil {
		return Agreement{}, err
	}
	if remote.ProtocolMin < 1 || remote.ProtocolMax < remote.ProtocolMin {
		return Agreement{}, errors.New("remote DUBI protocol range is invalid")
	}
	version := local.ProtocolMax
	if remote.ProtocolMax < version {
		version = remote.ProtocolMax
	}
	if version < local.ProtocolMin || version < remote.ProtocolMin {
		return Agreement{}, errors.New("DUBI protocol versions do not overlap")
	}
	if remote.StateFormat != local.StateFormat || remote.PackFormat != local.PackFormat || remote.RecordIndex != local.RecordIndex {
		return Agreement{}, errors.New("DUBI durable formats do not match")
	}
	return Agreement{Version: version, StateFormat: local.StateFormat, PackFormat: local.PackFormat, RecordIndex: local.RecordIndex}, nil
}
