package common

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestForkVersionScriptFormat guards FORK-05: fork builds report
// fork-vX.Y.Z+up.<9-char upstream commit> through common.Version.
func TestForkVersionScriptFormat(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh not available")
	}
	script := filepath.Join("..", "scripts", "fork-version.sh")
	out, err := exec.Command(sh, script).Output()
	require.NoError(t, err)
	version := strings.TrimSpace(string(out))
	require.Regexp(t, regexp.MustCompile(`^fork-v\d+\.\d+\.\d+\+up\.[0-9a-f]{9}$`), version)

	upstream, err := os.ReadFile(filepath.Join("..", "UPSTREAM_COMMIT"))
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(version, "+up."+strings.TrimSpace(string(upstream))[:9]))
}
