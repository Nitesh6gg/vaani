// Package version carries the engine's build version, reported as
// service.version in every log line (see internal/config/logging.go).
// Override at build time with:
//
//	go build -ldflags "-X github.com/nitesh/vaani/internal/version.Version=2.2.0"
package version

// Version is the Vaani engine's semantic version.
var Version = "2.1.0"
