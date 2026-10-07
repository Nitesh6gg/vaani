package config

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStartupBannerHasNoSecrets(t *testing.T) {
	cfg := Config{
		AppMode:        "agent",
		AriURL:         "http://ariuser:aripass@10.0.0.1:8088/ari",
		AriUser:        "ariuser",
		AriPass:        "aripass",
		AriApp:         "vaani",
		DograhDBURL:    "postgresql://dbuser:dbpass@10.0.0.2:5432/dograh?password=qpass",
		MinioEndpoint:  "10.0.0.2:9000",
		MinioAccessKey: "minioaccess",
		MinioSecretKey: "miniosecret",
		MinioBucket:    "voice-audio",
	}
	var buf bytes.Buffer
	writeBanner(&buf, startupFields(cfg))
	out := buf.String()

	for _, secret := range []string{"aripass", "dbuser", "dbpass", "qpass", "minioaccess", "miniosecret"} {
		assert.NotContains(t, out, secret)
	}
	assert.Contains(t, out, "http://10.0.0.1:8088/ari app=vaani")
	assert.Contains(t, out, "db=postgresql://10.0.0.2:5432/dograh")
}

func TestSafeURL(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", "-"},
		{"http://u:p@h:1/x?k=v", "http://h:1/x"},
		{"not a url", "(hidden)"},
		{"postgres://u:p@[::1", "(hidden)"},
	} {
		assert.Equal(t, tc.want, SafeURL(tc.in), tc.in)
	}
}
