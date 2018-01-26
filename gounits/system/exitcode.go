package system

import (
	"fmt"
	"os/exec"
	"syscall"
)

//GetExitCode is exported
func GetExitCode(err error) (int, error) {

	exitcode := 0
	if exiterr, ok := err.(*exec.ExitError); ok {
		if procexit, ok := exiterr.Sys().(syscall.WaitStatus); ok {
			return procexit.ExitStatus(), nil
		}
	}
	return exitcode, fmt.Errorf("failed to get exit code.")
}

//ErrorExitCode is exported
func ErrorExitCode(err error) int {

	if err != nil {
		var (
			exitcode int
			exiterr  error
		)
		if exitcode, exiterr = GetExitCode(err); exiterr != nil {
			return 127
		}
		return exitcode
	}
	return 0
}
