package ari

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/config"
)

var uuidV4Pattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewUUIDv4_MatchesRFC4122Format(t *testing.T) {
	seen := make(map[string]bool)

	for i := 0; i < 100; i++ {
		id, err := newUUIDv4()
		require.NoError(t, err)
		assert.Regexp(t, uuidV4Pattern, id, "must be a valid version-4, variant-1 UUID string")
		assert.False(t, seen[id], "must not repeat across 100 generations")
		seen[id] = true
	}
}

// TestCreateAudioSocketExternalMedia_SendsRequiredDataField is the direct
// regression test for the production bug: Asterisk's chan_audiosocket rejects
// externalMedia with encapsulation=audiosocket unless the request body includes a
// non-empty "data" field (confirmed live via `ari set debug on`: "400 Bad
// Request: data can not be empty"). The vendored ari.ExternalMediaOptions struct
// has no such field, which is why this bypasses it with a raw request -- this
// test proves that raw request actually carries everything Asterisk needs.
func TestCreateAudioSocketExternalMedia_SendsRequiredDataField(t *testing.T) {
	var (
		gotPath   string
		gotMethod string
		gotUser   string
		gotPass   string
		gotBody   map[string]any
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		gotUser, gotPass, _ = r.BasicAuth()

		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := config.Config{
		AriURL:  srv.URL,
		AriUser: "vaani",
		AriPass: "vaani",
	}

	err := createAudioSocketExternalMedia(cfg, "ext-chan-1", "vaani", "127.0.0.1:20000", "123e4567-e89b-12d3-a456-426614174000")
	require.NoError(t, err)

	assert.Equal(t, "/channels/externalMedia", gotPath)
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "vaani", gotUser)
	assert.Equal(t, "vaani", gotPass)

	assert.Equal(t, "ext-chan-1", gotBody["channelId"])
	assert.Equal(t, "vaani", gotBody["app"])
	assert.Equal(t, "127.0.0.1:20000", gotBody["external_host"])
	assert.Equal(t, "audiosocket", gotBody["encapsulation"])
	assert.Equal(t, "tcp", gotBody["transport"])
	assert.Equal(t, "client", gotBody["connection_type"])
	assert.Equal(t, "slin16", gotBody["format"])
	assert.Equal(t, "both", gotBody["direction"])
	assert.Equal(t, "123e4567-e89b-12d3-a456-426614174000", gotBody["data"],
		`"data" must be set and non-empty -- Asterisk rejects the request without it`)
}

func TestCreateAudioSocketExternalMedia_PropagatesNon2xxAsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"data can not be empty"}`))
	}))
	defer srv.Close()

	cfg := config.Config{AriURL: srv.URL, AriUser: "vaani", AriPass: "vaani"}

	err := createAudioSocketExternalMedia(cfg, "ext-chan-1", "vaani", "127.0.0.1:20000", "")
	assert.Error(t, err)
}
