package raft

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

func TestGroupRegistryRejectsLostOrReplacedResources(t *testing.T) {
	for _, damage := range []string{"parent-missing", "group-missing", "group-empty", "group-symlink", "registry-missing", "registry-symlink", "registry-corrupt", "marker-missing", "marker-symlink", "marker-foreign"} {
		t.Run(damage, func(t *testing.T) {
			store := t.TempDir()
			root, err := PrepareGroupRoot(store, "regional")
			if err != nil {
				t.Fatal(err)
			}
			registry := filepath.Join(store, groupRegistryFile)
			marker := filepath.Join(root, identityFile)
			switch damage {
			case "parent-missing":
				err = os.Rename(filepath.Join(store, groupDirectory), filepath.Join(store, groupDirectory+".retained"))
			case "group-missing", "group-empty", "group-symlink":
				err = os.Rename(root, root+".retained")
				if err == nil && damage == "group-empty" {
					err = os.Mkdir(root, 0o750)
				}
				if err == nil && damage == "group-symlink" {
					err = os.Symlink(root+".retained", root)
				}
			case "registry-missing", "registry-symlink":
				err = os.Rename(registry, registry+".retained")
				if err == nil && damage == "registry-symlink" {
					err = os.Symlink(registry+".retained", registry)
				}
			case "registry-corrupt":
				err = os.WriteFile(registry, []byte("{}"), 0o640)
			case "marker-missing", "marker-symlink":
				err = os.Rename(marker, marker+".retained")
				if err == nil && damage == "marker-symlink" {
					err = os.Symlink(marker+".retained", marker)
				}
			case "marker-foreign":
				foreign, prepareErr := PrepareGroupRoot(t.TempDir(), "regional")
				if prepareErr != nil {
					t.Fatal(prepareErr)
				}
				var data []byte
				data, err = os.ReadFile(filepath.Join(foreign, identityFile))
				if err == nil {
					err = os.WriteFile(marker, data, 0o640)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := PrepareGroupRoot(store, "regional"); err == nil {
				t.Fatal("missing/replaced Group silently adopted")
			}
			if damage == "parent-missing" || damage == "group-missing" {
				if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("lost Group recreated", err)
				}
			}
			if damage == "group-empty" {
				entries, err := os.ReadDir(root)
				if err != nil || len(entries) != 0 {
					t.Fatal("replacement initialized", entries, err)
				}
			}
			if exists, err := HasGroupResources(store); err != nil || !exists {
				t.Fatal("lost resource registration allowed fresh enrollment", exists, err)
			}
		})
	}
}

func TestGroupRegistryParallelCreationPreservesEveryRecord(t *testing.T) {
	store := t.TempDir()
	var wg sync.WaitGroup
	failures := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := PrepareGroupRoot(store, fmt.Sprintf("group-%d", i))
			failures <- err
		}(i)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	before, err := os.ReadFile(filepath.Join(store, groupRegistryFile))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		if _, err := PrepareGroupRoot(store, fmt.Sprintf("group-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.ReadFile(filepath.Join(store, groupRegistryFile))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("reopen changed catalog", err)
	}
}

func TestGroupRegistryPendingReplacementRejected(t *testing.T) {
	store := t.TempDir()
	_, err := prepareRegisteredGroup(store, "regional", func(at string) error {
		if at == "after-intent" {
			return errors.New("interrupted")
		}
		return nil
	})
	if err == nil {
		t.Fatal("hook not reached")
	}
	root := filepath.Join(store, groupDirectory, "regional")
	if err := os.Rename(root, root+".retained"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareGroupRoot(store, "regional"); err == nil {
		t.Fatal("pending Group adopted replacement inode")
	}
}

func TestGroupRegistryProcessCrash(t *testing.T) {
	if stage := os.Getenv("DUBI_TEST_GROUP_REGISTRY_STAGE"); stage != "" {
		_, err := prepareRegisteredGroup(os.Getenv("DUBI_TEST_GROUP_REGISTRY_ROOT"), "regional", func(at string) error {
			if at == stage {
				os.Exit(74)
			}
			return nil
		})
		t.Fatalf("crash stage not reached: %v", err)
	}
	for _, stage := range []string{"after-registry", "after-directory", "after-intent", "after-owner", "after-active"} {
		t.Run(stage, func(t *testing.T) {
			store := t.TempDir()
			command := exec.Command(os.Args[0], "-test.run=^TestGroupRegistryProcessCrash$")
			command.Env = append(os.Environ(), "DUBI_TEST_GROUP_REGISTRY_STAGE="+stage, "DUBI_TEST_GROUP_REGISTRY_ROOT="+store)
			output, err := command.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 74 {
				t.Fatalf("child: %v\n%s", err, output)
			}
			root, err := PrepareGroupRoot(store, "regional")
			if err != nil {
				t.Fatal("retry after process exit", err)
			}
			owner, err := os.ReadFile(filepath.Join(root, identityFile))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := PrepareGroupRoot(store, "regional"); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(filepath.Join(root, identityFile))
			if err != nil || !bytes.Equal(owner, after) {
				t.Fatal("owner regenerated", err)
			}
		})
	}
}
