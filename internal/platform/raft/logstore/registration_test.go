package logstore

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/storage/pebbleutil"
	"go.etcd.io/raft/v3/raftpb"
)

func TestLogRegistrationRejectsMissingOrReplacedDatabase(t *testing.T) {
	for _, damage := range []string{"missing-directory", "empty-directory", "empty-database", "foreign-database", "symlink-directory", "missing-registration", "bad-registration", "symlink-registration", "missing-inner-identity", "foreign-inner-identity"} {
		t.Run(damage, func(t *testing.T) {
			root := t.TempDir()
			s, err := Open(root)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Save(hard(1, 1, 1), []*raftpb.Entry{entry(1, 1, "durable")}, nil); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			directory := filepath.Join(root, logDirectory)
			marker := filepath.Join(root, logRegistrationFile)
			if strings.HasSuffix(damage, "directory") || damage == "empty-database" || damage == "foreign-database" {
				if err := os.Rename(directory, directory+".retained"); err != nil {
					t.Fatal(err)
				}
			}
			switch damage {
			case "empty-directory":
				err = os.Mkdir(directory, 0o750)
			case "symlink-directory":
				err = os.Symlink(directory+".retained", directory)
			case "empty-database":
				var db *pebble.DB
				db, err = pebbleutil.Open(directory, &pebble.Options{})
				if err == nil {
					err = db.Close()
				}
			case "foreign-database":
				foreign := t.TempDir()
				var other *Storage
				other, err = Open(foreign)
				if err == nil {
					err = other.Close()
				}
				if err == nil {
					err = os.Rename(filepath.Join(foreign, logDirectory), directory)
				}
			case "missing-registration", "symlink-registration":
				err = os.Rename(marker, marker+".retained")
				if err == nil && damage == "symlink-registration" {
					err = os.Symlink(marker+".retained", marker)
				}
			case "bad-registration":
				err = os.WriteFile(marker, []byte(`{}`), 0o640)
			case "missing-inner-identity", "foreign-inner-identity":
				var db *pebble.DB
				db, err = pebbleutil.Open(directory, &pebble.Options{})
				if err == nil {
					if damage == "missing-inner-identity" {
						err = db.Delete(logIdentityKey, pebble.Sync)
					} else {
						err = db.Set(logIdentityKey, []byte("other"), pebble.Sync)
					}
					err = errors.Join(err, db.Close())
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			opened, err := Open(root)
			if err == nil {
				opened.Close()
				t.Fatal("damaged Raft storage was opened")
			}
			if damage == "missing-directory" {
				if _, err := os.Lstat(directory); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("missing directory was recreated", err)
				}
			}
			if damage == "empty-directory" {
				entries, err := os.ReadDir(directory)
				if err != nil || len(entries) != 0 {
					t.Fatal("empty replacement was initialized", entries, err)
				}
			}
		})
	}
}

func TestLogRegistrationInitializationRetryAndLease(t *testing.T) {
	for _, stage := range []string{"after-directory", "after-intent", "after-database", "after-identity", "after-active"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			injected := errors.New("stop initialization")
			db, release, err := openRegisteredLog(root, "", func(at string) error {
				if at == stage {
					return injected
				}
				return nil
			})
			if !errors.Is(err, injected) || db != nil || release != nil {
				t.Fatal(db, err)
			}
			s, err := Open(root)
			if err != nil {
				t.Fatal("retry", err)
			}
			marker, err := os.ReadFile(filepath.Join(root, logRegistrationFile))
			if err != nil {
				t.Fatal(err)
			}
			if second, err := Open(root); err == nil {
				second.Close()
				t.Fatal("concurrent open succeeded")
			}
			if err := s.Save(hard(1, 1, 1), []*raftpb.Entry{entry(1, 1, "committed")}, nil); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			after, err := os.ReadFile(filepath.Join(root, logRegistrationFile))
			if err != nil || !bytes.Equal(marker, after) {
				t.Fatal("registration regenerated", err)
			}
			if last, err := s.LastIndex(); err != nil || last != 1 {
				t.Fatal(last, err)
			}
		})
	}
}

func TestLogRegistrationPendingRejectsReplacementAndUnownedRecords(t *testing.T) {
	for _, damage := range []string{"directory", "records"} {
		t.Run(damage, func(t *testing.T) {
			root := t.TempDir()
			_, _, err := openRegisteredLog(root, "", func(stage string) error {
				if stage == "after-database" {
					return errors.New("interrupt")
				}
				return nil
			})
			if err == nil {
				t.Fatal("hook not reached")
			}
			directory := filepath.Join(root, logDirectory)
			if damage == "directory" {
				if err := os.Rename(directory, directory+".retained"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(directory, 0o750); err != nil {
					t.Fatal(err)
				}
			} else {
				db, err := pebbleutil.Open(directory, &pebble.Options{})
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Set([]byte("foreign"), []byte("record"), pebble.Sync); err != nil {
					t.Fatal(err)
				}
				db.Close()
			}
			if s, err := Open(root); err == nil {
				s.Close()
				t.Fatal("unsafe pending initialization resumed")
			}
		})
	}
}

func TestLogRegistrationProcessCrash(t *testing.T) {
	if stage := os.Getenv("DUBI_TEST_RAFT_REGISTRATION_STAGE"); stage != "" {
		_, _, err := openRegisteredLog(os.Getenv("DUBI_TEST_RAFT_REGISTRATION_ROOT"), "", func(at string) error {
			if at == stage {
				os.Exit(73)
			}
			return nil
		})
		t.Fatalf("crash stage not reached: %v", err)
	}
	for _, stage := range []string{"after-directory", "after-intent", "after-database", "after-identity", "after-active"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestLogRegistrationProcessCrash$")
			command.Env = append(os.Environ(), "DUBI_TEST_RAFT_REGISTRATION_STAGE="+stage, "DUBI_TEST_RAFT_REGISTRATION_ROOT="+root)
			output, err := command.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 73 {
				t.Fatalf("child: %v\n%s", err, output)
			}
			s, err := Open(root)
			if err != nil {
				t.Fatal("crash recovery", err)
			}
			if err := s.Save(hard(1, 1, 1), []*raftpb.Entry{entry(1, 1, "after-crash")}, nil); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(root)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if last, err := s.LastIndex(); err != nil || last != 1 {
				t.Fatal(last, err)
			}
		})
	}
}
