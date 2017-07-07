// HackDay Pivotal Data R&D 2017

// Package pg_ctl wraps the postgres management utility of the same name
//
// It hopes to support the full functionality; see the Postgres documentation here:
// https://www.postgresql.org/docs/current/static/app-pg-ctl.html
//
package pg_ctl

import (
	"bytes"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"syscall"
)

var pg_ctl_bin = "/usr/local/gpdb/bin/pg_ctl"

// Controller holds information necessary for pg_ctl calls
type Controller struct {
	dataDir string
}

// Status is a convenient representation of the results from `pg_ctl status`
type Status struct {
	// ErrorCode is the error code from running `pg_ctl status`.
	//
	// Golang is very cautious about calculating error codes, because of portability concerns.
	// We assume a Unix-like architecture and do the heavy lifting for you.
	//
	// This field should always be set. At the very least, it reports 127 for an internal error.
	ErrorCode int

	// RawStdOut is the command's stdout as a string
	//
	// Even if the process isn't found, that's reported here on stdout.
	RawStdOut string

	// RawStdErr is the command's stderr as a string
	//
	// It will report unrecognized flags or worse errors in operation
	RawStdErr string

	// IsServerRunning is true only if a postgres process is found for the dataDir
	IsServerRunning bool

	// Pid is the ID for the postgres process found for the dataDir
	Pid int

	// PsPostgres is the command that started postgres, as a string
	PsPostgres string
}

// NewController is a factory, making a Controller that can be used for pg_ctl calls against a particular dataDir
func NewController(dataDir string) *Controller {
	return &Controller{
		dataDir: dataDir,
	}
}

// Status executes a vanilla `pg_ctl status`
//
// it waits for the command to finish before returning
func (p *Controller) Status() (Status, error) {
	cmd := exec.Command(pg_ctl_bin, "status", "-w", "-D", p.dataDir)
	var outbuf, errbuf bytes.Buffer
	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf

	err := cmd.Run()

	var errorCode int
	if err != nil {
		exiterr, ok := err.(*exec.ExitError)
		if ! ok {
			return Status{ErrorCode: 127}, err
		}
		waitStatus, ok := exiterr.Sys().(syscall.WaitStatus)
		if ! ok {
			return Status{ErrorCode: 127}, err
		}
		errorCode = waitStatus.ExitStatus()
	} else {
		errorCode = 0
	}

	rawStdOut := outbuf.String()
	firstLine, _ := outbuf.ReadString('\n')

	isServerRunning := strings.Contains(firstLine, "server is running")

	var pid int
	pidRegex := regexp.MustCompile(`PID: (\d+)`)
	pidMatches := pidRegex.FindStringSubmatch(firstLine)
	if pidMatches == nil {
		pid = 0
	} else {
		pid, _ = strconv.Atoi(pidMatches[1])
	}

	var psPostgres string
	if isServerRunning {
		secondLine, _ := outbuf.ReadString('\n')
		psPostgres = strings.TrimSpace(secondLine)
	} else {
		psPostgres = ""
	}

	status := Status{
		ErrorCode: errorCode,
		RawStdOut: rawStdOut,
		RawStdErr: errbuf.String(),
		IsServerRunning: isServerRunning,
		Pid: pid,
		PsPostgres: psPostgres,
	}
	return status, nil
}

// IsStarted reports whether a postgres instance has started against a dataDir
//
// Deprecated: Use Status.IsServerRunning instead
func (p *Controller) IsStarted() (bool, error) {
	cmd := exec.Command(pg_ctl_bin, "status", "-w", "-D", p.dataDir, "-o", "-c unix_socket_directories=/tmp")
	_, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			status := cmd.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()
			if status == 3 {
				return false, nil
			}
		}
		return false, fmt.Errorf("cannot get instance state: %v", err)
	}
	return true, nil
}
