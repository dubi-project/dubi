package rootstate

import (
	"path/filepath"
	"testing"

	repository "github.com/dubi-project/dubi/internal/platform/recovery/repository"
)

func TestRepositoryLifecycleAndDefault(t *testing.T) {
	state := rootFileFixture(t)
	local := repository.Repository{Name: "local", Type: repository.TypeFile, Path: "/backup/dubi",
		EncryptedCredentials: []byte("ciphertext"), CredentialsNonce: []byte("123456789012"), CredentialsAlgorithm: "aes-256-gcm", CredentialsKeyVersion: 1}
	applyRootFileCommand(t, state, Command{Type: CreateRepository, Repository: &local})
	if got, found, err := state.Repository("local"); err != nil || !found || got.Name != local.Name || got.Type != local.Type || got.Path != local.Path {
		t.Fatalf("Repository = %+v, %v, %v", got, found, err)
	}
	applyRootFileCommand(t, state, Command{Type: SetDefaultRepository, RepositoryDefault: &RepositoryDefault{Name: "local"}})
	if got, found, err := state.DefaultRepository(); err != nil || !found || got.Name != "local" {
		t.Fatalf("default Repository = %+v, %v, %v", got, found, err)
	}
	updated := local
	updated.Path = "/backup/dubi-v2"
	updated.EncryptedCredentials = nil
	updated.CredentialsNonce = nil
	updated.CredentialsAlgorithm = ""
	updated.CredentialsKeyVersion = 0
	applyRootFileCommand(t, state, Command{Type: UpdateRepository, Repository: &updated})
	if got, found, err := state.DefaultRepository(); err != nil || !found || got.Path != updated.Path || string(got.EncryptedCredentials) != "ciphertext" {
		t.Fatalf("updated default Repository = %+v, %v, %v", got, found, err)
	}
	applyRootFileCommand(t, state, Command{Type: DeleteRepository, RepositoryDelete: &RepositoryDelete{Name: "local"}})
	if _, found, err := state.Repository("local"); err != nil || found {
		t.Fatalf("deleted Repository = %v, %v", found, err)
	}
	if _, found, err := state.DefaultRepository(); err != nil || found {
		t.Fatalf("deleted default Repository = %v, %v", found, err)
	}
}

func TestRepositoryRejectsInvalidBackendOptions(t *testing.T) {
	state := rootFileFixture(t)
	bad := []repository.Repository{
		{Name: "bad", Type: repository.TypeFile, Path: "relative"},
		{Name: "bad", Type: repository.TypeFile, Path: "/backup", Bucket: "unexpected"},
		{Name: "bad", Type: repository.TypeS3, Bucket: "bucket", Endpoint: "file:///tmp"},
	}
	for index, value := range bad {
		command := Command{Type: CreateRepository, Repository: &value}
		command.Version = FormatVersion
		command.RequestID = "bad-repository-" + string(rune('a'+index))
		if _, err := EncodeCommand(command); err == nil {
			t.Fatalf("invalid Repository %d accepted", index)
		}
	}
	if _, err := EncodeCommand(Command{Version: FormatVersion, RequestID: "missing-repository", Type: SetDefaultRepository,
		RepositoryDefault: &RepositoryDefault{Name: "missing"}}); err != nil {
		// Encoding validates shape only; existence is a state-machine concern.
		t.Fatal(err)
	}
	command := Command{Version: FormatVersion, RequestID: "missing-default", Type: SetDefaultRepository,
		RepositoryDefault: &RepositoryDefault{Name: "missing"}}
	encoded, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Apply(state.AppliedIndex()+1, encoded); err != nil {
		t.Fatal(err)
	}
	if result, found, err := state.Result(command.RequestID); err != nil || !found || result.Error == "" {
		t.Fatalf("missing default result = %+v, %v, %v", result, found, err)
	}
}

func TestRepositorySurvivesRootSnapshot(t *testing.T) {
	source := rootFileFixture(t)
	value := repository.Repository{Name: "primary", Type: repository.TypeS3, Bucket: "cluster-backup", Prefix: "dubi", Region: "us-east-1"}
	applyRootFileCommand(t, source, Command{Type: CreateRepository, Repository: &value})
	applyRootFileCommand(t, source, Command{Type: SetDefaultRepository, RepositoryDefault: &RepositoryDefault{Name: value.Name}})
	image, index, conf, err := source.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	destination, err := Open(filepath.Join(t.TempDir(), "restored"))
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	if err := destination.Restore(index, image, conf); err != nil {
		t.Fatal(err)
	}
	got, found, err := destination.DefaultRepository()
	if err != nil || !found || got.Name != value.Name || got.Bucket != value.Bucket {
		t.Fatalf("snapshot Repository = %+v, %v, %v", got, found, err)
	}
}
