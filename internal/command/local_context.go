package command

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/server"
)

const localContextVersion = 1
const defaultLocalServer = "127.0.0.1:7443"

// Only a pointer and a local endpoint live in the user's home. The durable
// Node identity, cluster topology, certificates and volumes stay in Store.
type localContext struct {
	Version   int    `json:"version"`
	Store     string `json:"store"`
	NodeID    string `json:"node_id"`
	ClusterID string `json:"cluster_id,omitempty"`
	Listen    string `json:"listen,omitempty"`
	Server    string `json:"server,omitempty"`
	Advertise string `json:"advertise,omitempty"`
}

func localContextPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "", errors.Join(err, errors.New("cannot locate user home for ~/.dubi"))
	}
	return filepath.Join(home, ".dubi", "current.json"), nil
}

func readLocalContext() (localContext, bool, error) {
	path, err := localContextPath()
	if err != nil {
		return localContext{}, false, err
	}
	directory := filepath.Dir(path)
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		return localContext{}, false, nil
	}
	if err != nil {
		return localContext{}, false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return localContext{}, false, errors.New("~/.dubi is not a physical directory")
	}
	info, err = os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return localContext{}, false, nil
	}
	if err != nil {
		return localContext{}, false, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 || info.Size() > 4096 {
		return localContext{}, false, errors.New("invalid ~/.dubi/current.json file")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return localContext{}, false, err
	}
	var current localContext
	if err := json.Unmarshal(encoded, &current); err != nil || current.Version != localContextVersion ||
		!filepath.IsAbs(current.Store) || current.NodeID == "" ||
		(current.Listen != "" && config.ValidateListen(current.Listen) != nil) ||
		(current.Server != "" && identityenrollment.ValidateNodeAddress(current.Server) != nil) ||
		(current.Advertise != "" && identityenrollment.ValidateNodeAddress(current.Advertise) != nil) {
		return localContext{}, false, errors.New("invalid ~/.dubi/current.json; choose an explicit --store")
	}
	return current, true, nil
}

// A missing context uses the historical default. An invalid/stale pointer is
// never interpreted as permission to initialize an empty replacement Store.
func resolveStore(flags *flag.FlagSet, raw string) (string, error) {
	if flagWasSet(flags, "store") {
		if strings.TrimSpace(raw) == "" {
			return "", errors.New("--store cannot be empty")
		}
		return raw, nil
	}
	if value := strings.TrimSpace(os.Getenv("DUBI_STORE")); value != "" {
		return value, nil
	}
	current, found, err := readLocalContext()
	if err != nil {
		return "", err
	}
	if !found {
		return config.DefaultStore(), nil
	}
	value, err := identity.Load(current.Store)
	if err != nil || value.NodeID != current.NodeID || (current.ClusterID != "" && value.ClusterID != current.ClusterID) {
		return "", fmt.Errorf("~/.dubi/current.json does not match its existing Node Store; use --store explicitly: %w", errors.Join(err, errors.New("local Node identity mismatch")))
	}
	return current.Store, nil
}

func resolveServer(flags *flag.FlagSet, raw, store string) (string, error) {
	if flagWasSet(flags, "server") {
		return raw, nil
	}
	current, found, err := readLocalContext()
	if err != nil {
		// An explicit Store must remain usable to repair a broken context.
		if flagWasSet(flags, "store") || os.Getenv("DUBI_STORE") != "" {
			return defaultLocalServer, nil
		}
		return "", err
	}
	if !found || current.Server == "" {
		return defaultLocalServer, nil
	}
	abs, err := filepath.Abs(store)
	if err != nil {
		return "", err
	}
	if abs != current.Store {
		return defaultLocalServer, nil
	}
	return current.Server, nil
}

func resolveListen(flags *flag.FlagSet, raw, store string) (string, error) {
	if flagWasSet(flags, "listen") {
		return raw, nil
	}
	current, found, err := readLocalContext()
	if err != nil {
		if flagWasSet(flags, "store") || os.Getenv("DUBI_STORE") != "" {
			return config.DefaultListen, nil
		}
		return "", err
	}
	if !found || current.Listen == "" {
		return config.DefaultListen, nil
	}
	abs, err := filepath.Abs(store)
	if err != nil {
		return "", err
	}
	if abs != current.Store {
		return config.DefaultListen, nil
	}
	return current.Listen, nil
}

func resolveAdvertise(flags *flag.FlagSet, raw, store string) (string, error) {
	if flagWasSet(flags, "advertise") {
		return raw, nil
	}
	current, found, err := readLocalContext()
	if err != nil {
		if flagWasSet(flags, "store") || os.Getenv("DUBI_STORE") != "" {
			return "", nil
		}
		return "", err
	}
	if !found || current.Advertise == "" || flagWasSet(flags, "listen") {
		return "", nil
	}
	abs, err := filepath.Abs(store)
	if err != nil {
		return "", err
	}
	if abs != current.Store {
		return "", nil
	}
	return current.Advertise, nil
}

func flagWasSet(flags *flag.FlagSet, name string) bool {
	found := false
	flags.Visit(func(value *flag.Flag) {
		if value.Name == name {
			found = true
		}
	})
	return found
}

func saveLocalContext(store string, value identity.Identity, listen, endpoint, advertise string) error {
	if value.NodeID == "" || store == "" ||
		(listen != "" && config.ValidateListen(listen) != nil) ||
		(endpoint != "" && identityenrollment.ValidateNodeAddress(endpoint) != nil) ||
		(advertise != "" && identityenrollment.ValidateNodeAddress(advertise) != nil) {
		return errors.New("cannot save incomplete local Node context")
	}
	abs, err := filepath.Abs(store)
	if err != nil {
		return err
	}
	path, err := localContextPath()
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(err, errors.New("~/.dubi is not a physical directory"))
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return err
	}
	if previous, found, err := readLocalContext(); err == nil && found && previous.Store == abs && previous.NodeID == value.NodeID {
		if endpoint == "" {
			endpoint = previous.Server
		}
		if listen == "" {
			listen = previous.Listen
		}
		if advertise == "" {
			advertise = previous.Advertise
		}
	}
	current := localContext{Version: localContextVersion, Store: abs, NodeID: value.NodeID,
		ClusterID: value.ClusterID, Listen: listen, Server: endpoint, Advertise: advertise}
	encoded, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".current-*")
	if err != nil {
		return err
	}
	defer os.Remove(temporary.Name())
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(append(encoded, '\n')); err != nil {
		temporary.Close()
		return err
	}
	if err := errors.Join(temporary.Sync(), temporary.Close()); err != nil {
		return err
	}
	if err := os.Rename(temporary.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

// Local status/control connects to the actual listener, not the advertised
// peer route (which may be a NAT address). Wildcard binds use loopback.
func localEndpoint(process *server.Server) (string, error) {
	if process == nil {
		return "", errors.New("Node process is nil")
	}
	return localListenerEndpoint(process.LocalAddress())
}

func localListenerEndpoint(bound string) (string, error) {
	host, port, err := net.SplitHostPort(bound)
	if err != nil {
		return "", err
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		if ip.To4() == nil {
			host = "::1"
		} else {
			host = "127.0.0.1"
		}
	}
	address := net.JoinHostPort(host, port)
	if err := identityenrollment.ValidateNodeAddress(address); err != nil {
		return "", err
	}
	return address, nil
}
