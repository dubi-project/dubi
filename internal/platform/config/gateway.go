package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// GatewaySettings stores only local deployment inputs, never credentials.
type GatewaySettings struct {
	Host            string `json:"host,omitempty"`
	Managed         bool   `json:"managed,omitempty"`
	ACMEDirectory   string `json:"acme_directory,omitempty"`
	ChallengeListen string `json:"challenge_listen,omitempty"`
	Version         int    `json:"version"`
	Listen          string `json:"listen"`
	Certificate     string `json:"certificate,omitempty"`
	PrivateKey      string `json:"private_key,omitempty"`
}

func LoadGateway(store string) (GatewaySettings, error) {
	v := GatewaySettings{Version: 1}
	b, err := os.ReadFile(filepath.Join(store, "gateway.json"))
	if errors.Is(err, os.ErrNotExist) {
		return v, nil
	}
	if err != nil {
		return v, err
	}
	if len(b) > 16384 {
		return v, errors.New("gateway configuration exceeds limit")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(&v) != nil || d.Decode(new(any)) != io.EOF || v.Version != 1 {
		return v, errors.New("invalid gateway configuration")
	}
	if v.Listen != "" {
		if err := ValidateListen(v.Listen); err != nil {
			return v, err
		}
	}
	if (v.Certificate == "") != (v.PrivateKey == "") {
		return v, errors.New("incomplete gateway certificate settings")
	}
	return v, nil
}
func SaveGateway(store string, v GatewaySettings) error {
	v.Version = 1
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(store, ".gateway-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(append(b, '\n'))
	}
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(store, "gateway.json")); err != nil {
		return err
	}
	dir, err := os.Open(store)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
