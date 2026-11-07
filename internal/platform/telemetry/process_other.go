//go:build !linux && !darwin

package telemetry

func processUsage() (*float64, *uint64) { return nil, nil }
