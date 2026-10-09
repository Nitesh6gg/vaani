package agent

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackchannel(t *testing.T) {
	for _, tc := range []struct {
		text string
		want bool
	}{
		// Acknowledgements: the reply goes on.
		{"जी।", true}, // call 509's
		{"हाँ", true},
		{"हां जी", true},
		{"जी हाँ", true},
		{"हम्म", true},
		{"अच्छा अच्छा", true},
		{"ठीक है।", true},
		{"ओके", true},
		{"Okay.", true},
		{"hmmmm", true},
		{"Haan ji", true},
		{"achha theek hai", true},
		{"yes, yes", true},
		{"हो", true},             // Marathi
		{"ठीक आहे", true},        // Marathi
		{"હા બરાબર", true},       // Gujarati
		{"হ্যাঁ, ঠিক আছে", true}, // Bengali
		{"ਹਾਂਜੀ", true},          // Punjabi
		{"ହଁ", true},             // Odia
		{"சரி சரி", true},        // Tamil
		{"ஆமா", true},
		{"హా సరే", true}, // Telugu
		{"ಹೌದು", true},   // Kannada
		{"ശരി", true},    // Malayalam
		{"جی ہاں", true}, // Urdu

		// Refusing, asking, stopping, or saying anything more: an interruption.
		{"नहीं", false},
		{"जी नहीं", false},
		{"हाँ लेकिन रुकिए", false},
		{"क्या?", false},
		{"सॉरी क्या बोला?", false}, // call 509's real interruption
		{"महंगा है।", false},       // an answer over the agent (call 509)
		{"ok wait", false},
		{"no", false},
		{"இல்லை", false}, // Tamil "no"
		{"లేదు", false},  // Telugu "no"
		{"5", false},
		{"", false},
		{"।", false},
	} {
		assert.Equal(t, tc.want, backchannel(tc.text), "%q", tc.text)
	}
}

func TestSqueezeRepeats(t *testing.T) {
	assert.Equal(t, "hmm", squeezeRepeats("hmmmmm"))
	assert.Equal(t, "okk", squeezeRepeats("okkkk"))
	assert.Equal(t, "achha", squeezeRepeats("achha"), "a real double letter stays")
	assert.Equal(t, "हम्म", squeezeRepeats("हम्म"))
}
