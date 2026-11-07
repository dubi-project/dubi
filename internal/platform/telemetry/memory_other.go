//go:build !linux

package telemetry

func physicalMemory() *uint64 { return nil }
