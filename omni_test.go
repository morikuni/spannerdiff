package spannerdiff

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"
)

// omniContainerEnv is the environment variable of the name of the Docker container that runs Spanner Omni.
// The tests verify the generated DDLs on Spanner Omni when it is set.
const omniContainerEnv = "SPANNER_OMNI_CONTAINER"

// omni runs the CLI of Spanner Omni in its container.
// Spanner Omni has no REST API, and the CLI is used instead of the client library
// to avoid adding its dependencies to this module.
type omni struct {
	container   string
	databaseSeq atomic.Int64
}

var sharedOmni = &omni{container: os.Getenv(omniContainerEnv)}

// omniRun makes the names of databases different from those of the previous runs on the same container.
var omniRun = time.Now().Unix()

// run runs the CLI with the DDLs as the file of --ddl-file, if any.
func (o *omni) run(ddls []string, args ...string) (string, error) {
	// The default timeout of the CLI is too short when many schema changes run in parallel.
	args = append([]string{"exec", "-i", o.container, "/google/spanner/bin/spanner"}, append(args, "--timeout", "30m")...)
	cmd := exec.Command("docker", args...)
	if len(ddls) > 0 {
		cmd.Args = append(cmd.Args, "--ddl-file", "/dev/stdin")
		cmd.Stdin = strings.NewReader(strings.Join(ddls, ";\n") + ";\n")
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		// The error follows the warning about the license.
		msg := stderr.String()
		if i := strings.Index(msg, "Error: "); i >= 0 {
			msg = msg[i+len("Error: "):]
		}
		return "", fmt.Errorf("%s: %w", strings.TrimSpace(msg), err)
	}
	// The CLI overwrites the line of its progress by the result.
	out := stdout.String()
	if i := strings.LastIndex(out, "\x1b[K"); i >= 0 {
		out = out[i+len("\x1b[K"):]
	}
	return out, nil
}

// createDatabase creates a database with the DDLs and returns its name.
func (o *omni) createDatabase(ddls []string) (string, error) {
	name := fmt.Sprintf("db%d-%d", omniRun, o.databaseSeq.Add(1))
	if _, err := o.run(ddls, "databases", "create", name); err != nil {
		return "", err
	}
	return name, nil
}

// dropDatabase doesn't drop the database, because it takes long while many schema changes run,
// and the container is removed after the tests.
func (o *omni) dropDatabase(string) error {
	return nil
}

func (o *omni) updateDDL(database string, ddls []string) error {
	_, err := o.run(ddls, "databases", "ddl", "update", database)
	return err
}

func (o *omni) getDDL(database string) (string, error) {
	out, err := o.run(nil, "databases", "ddl", "describe", database)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(out, "Listed 0 items.") {
		return "", nil
	}
	return out, nil
}

func (o *omni) knownFailures() map[string]string {
	return nil
}

func (o *omni) omitsRoles() bool {
	return false
}
