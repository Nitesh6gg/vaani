package dograh

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeToolUUIDs(t *testing.T) {
	uuids, err := nodeToolUUIDs([]byte(`{"nodes": [
		{"id": "1", "type": "startCall", "data": {"prompt": "hi", "tool_uuids": ["a", "b"]}},
		{"id": "2", "type": "agentNode", "data": {"tool_uuids": null}},
		{"id": "3", "type": "agentNode", "data": {"tool_uuids": ["b", "c"]}},
		{"id": "4", "type": "endCall", "data": {}}
	], "edges": []}`))
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, uuids, "distinct, in node order")

	_, err = nodeToolUUIDs([]byte(`not json`))
	assert.Error(t, err)
}
