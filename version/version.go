package version

import (
	"fmt"
	"runtime"
)

// Populated during build, don't touch!
var (
	Version   = "v0.0.0"
	GitRev    = "undefined"
	GitBranch = "undefined"
	BuildDate = "Fri, 17 Jun 1988 01:58:00 +0200"
	// GoVersion system go version.
	GoVersion = runtime.Version()
	// Platform info.
	Platform = fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
)
