package command

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	repository "github.com/dubi-project/dubi/internal/platform/recovery/repository"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
)

// runRepository manages the cluster-level destinations used by Backup and
// Restore. Repository configuration is committed through Root, so all
// subcommands can be issued against any current Node.
func runRepository(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: dubi repository add|list|show|update|remove|set-default|default ...")
	}
	switch args[0] {
	case "add", "update":
		return mutateRepository(ctx, args[0], args[1:], stdout, stderr)
	case "list":
		return listRepositories(ctx, args[1:], stdout, stderr, "")
	case "show":
		return listRepositories(ctx, args[1:], stdout, stderr, "show")
	case "remove":
		return removeRepository(ctx, args[1:], stdout, stderr)
	case "set-default", "default":
		return setDefaultRepository(ctx, args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown repository command %q", args[0])
	}
}

func repositoryFlags(name string, stderr io.Writer) (*flag.FlagSet, *string, *string, *string) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	store := flags.String("store", "", "existing Node store")
	server := flags.String("server", "", "reachable Node RPC address")
	requestID := flags.String("request-id", "", "stable ID for retrying an uncertain request")
	return flags, store, server, requestID
}

func mutateRepository(ctx context.Context, operation string, args []string, stdout, stderr io.Writer) error {
	flags, store, server, requestID := repositoryFlags("repository "+operation, stderr)
	typeName := flags.String("type", string(repository.TypeFile), "Repository backend: file or s3")
	path := flags.String("path", "", "absolute path for a file Repository")
	bucket := flags.String("bucket", "", "S3 bucket")
	prefix := flags.String("prefix", "", "optional S3 key prefix")
	endpoint := flags.String("endpoint", "", "optional S3-compatible HTTP(S) endpoint")
	region := flags.String("region", "", "S3 region")
	pathStyle := flags.Bool("path-style", false, "use S3 path-style requests")
	credentialsFile := flags.String("credentials-file", "", "0600 JSON file with access_key, secret_key and optional session_token")
	if err := flags.Parse(args); err != nil {
		return err
	}
	resolved, err := resolveStore(flags, *store)
	if err != nil {
		return err
	}
	*store = resolved
	address, err := resolveServer(flags, *server, *store)
	if err != nil {
		return err
	}
	*server = address
	if flags.NArg() != 1 || *store == "" || identityenrollment.ValidateNodeAddress(*server) != nil {
		return fmt.Errorf("usage: dubi repository %s [--store path] [--server host:port] [--request-id ID] [--type file|s3] [--path path] [--bucket bucket] [--prefix prefix] [--endpoint URL] [--region region] [--path-style] [--credentials-file JSON] <name>", operation)
	}
	if err := ensureRequestID(requestID); err != nil {
		return err
	}
	value := repository.Repository{Name: flags.Arg(0), Type: repository.Type(strings.ToLower(strings.TrimSpace(*typeName))), Path: *path,
		Bucket: *bucket, Prefix: *prefix, Endpoint: *endpoint, Region: *region, PathStyle: *pathStyle}
	if operation == "add" {
		if err := value.Validate(); err != nil {
			return fmt.Errorf("invalid Repository configuration: %w", err)
		}
	}
	conn, id, err := connectDataNode(ctx, *store, *server)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(stderr, "dubi: request-id %s\n", *requestID)
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client := datawire.NewDataServiceClient(conn)
	if operation == "update" && (!flagWasSet(flags, "type") || !flagWasSet(flags, "path") || !flagWasSet(flags, "bucket") ||
		!flagWasSet(flags, "prefix") || !flagWasSet(flags, "endpoint") || !flagWasSet(flags, "region") || !flagWasSet(flags, "path-style")) {
		listed, listErr := client.ListRepositories(call, &datawire.ListRepositoriesRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID})
		if listErr != nil {
			return listErr
		}
		var existing *datawire.RepositoryConfig
		for _, candidate := range listed.GetRepositories() {
			if candidate.GetName() == value.Name {
				existing = candidate
				break
			}
		}
		if existing == nil {
			return fmt.Errorf("Repository %q is not registered", value.Name)
		}
		if !flagWasSet(flags, "type") {
			value.Type = repository.Type(existing.GetType())
		}
		if !flagWasSet(flags, "path") {
			value.Path = existing.GetPath()
		}
		if !flagWasSet(flags, "bucket") {
			value.Bucket = existing.GetBucket()
		}
		if !flagWasSet(flags, "prefix") {
			value.Prefix = existing.GetPrefix()
		}
		if !flagWasSet(flags, "endpoint") {
			value.Endpoint = existing.GetEndpoint()
		}
		if !flagWasSet(flags, "region") {
			value.Region = existing.GetRegion()
		}
		if !flagWasSet(flags, "path-style") {
			value.PathStyle = existing.GetPathStyle()
		}
	}
	if *credentialsFile != "" {
		if !value.IsS3() {
			return errors.New("--credentials-file requires an S3 Repository")
		}
		key, keyErr := repository.LoadRepositoryKey(*store)
		if keyErr != nil {
			return fmt.Errorf("load local Repository key: %w", keyErr)
		}
		credentials, credentialErr := readS3CredentialsFile(*credentialsFile)
		if credentialErr != nil {
			return credentialErr
		}
		value, err = repository.SealS3Credentials(value, key, 1, credentials)
		if err != nil {
			return fmt.Errorf("encrypt Repository credentials: %w", err)
		}
	}
	if operation == "update" {
		if err := value.Validate(); err != nil {
			return fmt.Errorf("invalid Repository configuration: %w", err)
		}
	}
	wire := &datawire.RepositoryConfig{Name: value.Name, Type: string(value.Type), Path: value.Path,
		Bucket: value.Bucket, Prefix: value.Prefix, Endpoint: value.Endpoint, Region: value.Region, PathStyle: value.PathStyle,
		EncryptedCredentials: value.EncryptedCredentials, CredentialsNonce: value.CredentialsNonce,
		CredentialsAlgorithm: value.CredentialsAlgorithm, CredentialsKeyVersion: value.CredentialsKeyVersion}
	var response *datawire.RepositoryMutationResponse
	if operation == "add" {
		response, err = client.CreateRepository(call, &datawire.CreateRepositoryRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID, RequestId: *requestID, Repository: wire})
	} else {
		response, err = client.UpdateRepository(call, &datawire.UpdateRepositoryRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID, RequestId: *requestID, Repository: wire})
	}
	if err != nil {
		return err
	}
	return encodeRepositoryJSON(stdout, response)
}

func readS3CredentialsFile(path string) (repository.S3Credentials, error) {
	info, err := os.Stat(path)
	if err != nil {
		return repository.S3Credentials{}, fmt.Errorf("read S3 credentials file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() == 0 || info.Size() > 16<<10 {
		return repository.S3Credentials{}, errors.New("S3 credentials file must be a private regular file up to 16 KiB")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return repository.S3Credentials{}, fmt.Errorf("read S3 credentials file: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	var credentials repository.S3Credentials
	if err := decoder.Decode(&credentials); err != nil {
		return repository.S3Credentials{}, errors.New("invalid S3 credentials JSON")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return repository.S3Credentials{}, errors.New("invalid S3 credentials JSON")
	}
	return credentials, nil
}

func listRepositories(ctx context.Context, args []string, stdout, stderr io.Writer, wanted string) error {
	flags, store, server, _ := repositoryFlags("repository list", stderr)
	if err := flags.Parse(args); err != nil {
		return err
	}
	resolved, err := resolveStore(flags, *store)
	if err != nil {
		return err
	}
	*store = resolved
	address, err := resolveServer(flags, *server, *store)
	if err != nil {
		return err
	}
	*server = address
	if wanted == "show" {
		if flags.NArg() != 1 {
			return errors.New("usage: dubi repository show [--store path] [--server host:port] <name>")
		}
		wanted = flags.Arg(0)
	} else if flags.NArg() != 0 {
		return errors.New("usage: dubi repository list [--store path] [--server host:port]")
	}
	if *store == "" || identityenrollment.ValidateNodeAddress(*server) != nil {
		if wanted == "" {
			return errors.New("usage: dubi repository list [--store path] [--server host:port]")
		}
		return errors.New("usage: dubi repository show [--store path] [--server host:port] <name>")
	}
	conn, id, err := connectDataNode(ctx, *store, *server)
	if err != nil {
		return err
	}
	defer conn.Close()
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response, err := datawire.NewDataServiceClient(conn).ListRepositories(call, &datawire.ListRepositoriesRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID})
	if err != nil {
		return err
	}
	if wanted == "" {
		return encodeRepositoryJSON(stdout, response)
	}
	for _, value := range response.Repositories {
		if value.GetName() == wanted {
			return encodeRepositoryJSON(stdout, value)
		}
	}
	return fmt.Errorf("Repository %q is not registered", wanted)
}

func removeRepository(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags, store, server, requestID := repositoryFlags("repository remove", stderr)
	if err := flags.Parse(args); err != nil {
		return err
	}
	resolved, err := resolveStore(flags, *store)
	if err != nil {
		return err
	}
	*store = resolved
	address, err := resolveServer(flags, *server, *store)
	if err != nil {
		return err
	}
	*server = address
	if flags.NArg() != 1 || *store == "" || identityenrollment.ValidateNodeAddress(*server) != nil {
		return errors.New("usage: dubi repository remove [--store path] [--server host:port] [--request-id ID] <name>")
	}
	if err := repository.ValidateName(flags.Arg(0)); err != nil {
		return err
	}
	if err := ensureRequestID(requestID); err != nil {
		return err
	}
	conn, id, err := connectDataNode(ctx, *store, *server)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(stderr, "dubi: request-id %s\n", *requestID)
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response, err := datawire.NewDataServiceClient(conn).DeleteRepository(call, &datawire.DeleteRepositoryRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID, RequestId: *requestID, Name: flags.Arg(0)})
	if err != nil {
		return err
	}
	return encodeRepositoryJSON(stdout, response)
}

func setDefaultRepository(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags, store, server, requestID := repositoryFlags("repository set-default", stderr)
	if err := flags.Parse(args); err != nil {
		return err
	}
	resolved, err := resolveStore(flags, *store)
	if err != nil {
		return err
	}
	*store = resolved
	address, err := resolveServer(flags, *server, *store)
	if err != nil {
		return err
	}
	*server = address
	if flags.NArg() != 1 || *store == "" || identityenrollment.ValidateNodeAddress(*server) != nil {
		return errors.New("usage: dubi repository set-default [--store path] [--server host:port] [--request-id ID] <name>")
	}
	if err := repository.ValidateName(flags.Arg(0)); err != nil {
		return err
	}
	if err := ensureRequestID(requestID); err != nil {
		return err
	}
	conn, id, err := connectDataNode(ctx, *store, *server)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(stderr, "dubi: request-id %s\n", *requestID)
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response, err := datawire.NewDataServiceClient(conn).SetDefaultRepository(call, &datawire.SetDefaultRepositoryRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID, RequestId: *requestID, Name: flags.Arg(0)})
	if err != nil {
		return err
	}
	return encodeRepositoryJSON(stdout, response)
}

func encodeRepositoryJSON(stdout io.Writer, value any) error {
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
