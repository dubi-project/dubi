package command

import (
	"encoding/json"
	"errors"
	"flag"
	"github.com/dubi-project/dubi/internal/platform/config"
	"io"
	"os"
	"path/filepath"
	"time"
)

func runGateway(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "status" {
		return errors.New("usage: dubi gateway status [--store path]")
	}
	f := flag.NewFlagSet("gateway status", flag.ContinueOnError)
	f.SetOutput(stderr)
	store := f.String("store", "", "Node metadata directory")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("gateway status accepts flags only")
	}
	resolved, err := resolveStore(f, *store)
	if err != nil {
		return err
	}
	v, err := config.LoadGateway(resolved)
	if err != nil {
		return err
	}
	result := map[string]any{"enabled": v.Listen != "", "listen": v.Listen, "host": v.Host, "observed_unix": time.Now().Unix()}
	if v.Managed {
		result["certificate_mode"] = "managed"
		b, err := os.ReadFile(filepath.Join(resolved, "gateway-pki", "status.json"))
		if err == nil && len(b) < 16384 {
			var status config.CertificateStatus
			if err = json.Unmarshal(b, &status); err == nil {
				result["certificate"] = status
			}
		}
		result["status_source"] = "last saved local certificate observation; does not prove process is running"
	} else if v.Listen != "" {
		g, err := config.PrepareGatewayTLS(resolved, v)
		if err != nil {
			result["certificate_error"] = err.Error()
		} else if g.Status != nil {
			result["certificate"] = g.Status()
		}
	}
	return json.NewEncoder(stdout).Encode(result)
}
