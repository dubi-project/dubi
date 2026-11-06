package command

import (
	"flag"
	"path/filepath"

	"github.com/dubi-project/dubi/internal/platform/config"
)

func resolveGatewaySettings(f *flag.FlagSet, store string, listen, certificate, key *string) (config.GatewaySettings, error) {
	v, err := config.LoadGateway(store)
	if err != nil {
		return v, err
	}
	set := map[string]bool{}
	f.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if set["api-listen"] {
		v.Listen = *listen
	}
	if set["api-cert"] {
		v.Certificate = *certificate
	}
	if set["api-key"] {
		v.PrivateKey = *key
	}
	if set["api-host"] {
		v.Host = f.Lookup("api-host").Value.String()
	}
	if set["api-acme"] {
		v.Managed = f.Lookup("api-acme").Value.String() == "true"
		if v.Managed {
			v.Certificate = ""
			v.PrivateKey = ""
		}
	}
	if set["acme-directory"] {
		v.ACMEDirectory = f.Lookup("acme-directory").Value.String()
	}
	if set["acme-listen"] {
		v.ChallengeListen = f.Lookup("acme-listen").Value.String()
	}
	if v.Certificate != "" {
		v.Certificate, err = filepath.Abs(v.Certificate)
		if err != nil {
			return v, err
		}
	}
	if v.PrivateKey != "" {
		v.PrivateKey, err = filepath.Abs(v.PrivateKey)
		if err != nil {
			return v, err
		}
	}
	*listen, *certificate, *key = v.Listen, v.Certificate, v.PrivateKey
	return v, nil
}
