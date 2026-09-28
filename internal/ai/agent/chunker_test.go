package agent

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSentenceChunker_FlushesOnDelimiter(t *testing.T) {
	c := &SentenceChunker{}

	out := c.Feed("Hello there. ")

	assert.Equal(t, []string{"Hello there."}, out)
	assert.Equal(t, " ", string(c.buf))
}

func TestSentenceChunker_FlushesOnDevanagariDanda(t *testing.T) {
	c := &SentenceChunker{}

	out := c.Feed("नमस्ते।")

	assert.Equal(t, []string{"नमस्ते।"}, out)
}

func TestSentenceChunker_NoFlushUnderThresholdWithNoDelimiter(t *testing.T) {
	c := &SentenceChunker{}

	out := c.Feed("still thinking")

	assert.Empty(t, out)
	assert.Equal(t, "still thinking", string(c.buf))
}

func TestSentenceChunker_FlushesAtRuneThresholdNotByteThreshold(t *testing.T) {
	c := &SentenceChunker{}

	// 80 Devanagari runes, each 3 bytes (240 bytes) -- a byte-count threshold
	// of 80 would flush 3x too early; must flush only once 80 RUNES arrive.
	word := "नमस्ते"                         // 6 runes
	almostEighty := strings.Repeat(word, 13) // 78 runes, no delimiter

	out := c.Feed(almostEighty)
	assert.Empty(t, out, "must not flush before 80 runes")

	out = c.Feed("अब") // +2 runes = 80 total
	assert.Len(t, out, 1, "must flush once the 80-rune threshold is reached")
}

func TestSentenceChunker_MultipleDelimitersInOneFeedYieldMultipleChunks(t *testing.T) {
	c := &SentenceChunker{}

	out := c.Feed("One. Two! Three?")

	assert.Equal(t, []string{"One.", " Two!", " Three?"}, out)
}

func TestSentenceChunker_FeedAcrossMultipleCallsAccumulates(t *testing.T) {
	c := &SentenceChunker{}

	assert.Empty(t, c.Feed("Hel"))
	assert.Empty(t, c.Feed("lo"))
	out := c.Feed(".")

	assert.Equal(t, []string{"Hello."}, out)
}

func TestSentenceChunker_FlushReturnsRemainderAndClearsBuffer(t *testing.T) {
	c := &SentenceChunker{}

	c.Feed("trailing fragment no punctuation")

	assert.Equal(t, "trailing fragment no punctuation", c.Flush())
	assert.Equal(t, "", c.Flush(), "second Flush on an empty buffer returns empty")
}

func TestSentenceChunker_DoesNotFlushPunctuationOnlyFragment(t *testing.T) {
	c := &SentenceChunker{}

	// An ellipsis arriving token by token must not flush three separate
	// content-free "sentences" -- Sarvam's TTS rejects those with a 400.
	out := c.Feed(".")
	out = append(out, c.Feed(".")...)
	out = append(out, c.Feed(".")...)

	assert.Empty(t, out, "bare punctuation must not flush on its own")
	assert.Equal(t, "...", string(c.buf))
}

func TestSentenceChunker_AbsorbsLeadingPunctuationIntoNextSentence(t *testing.T) {
	c := &SentenceChunker{}

	c.Feed("...")
	out := c.Feed("Wow!")

	assert.Equal(t, []string{"...Wow!"}, out, "held-back punctuation must absorb into the next real content")
}

func TestSentenceChunker_FlushDropsPunctuationOnlyRemainder(t *testing.T) {
	c := &SentenceChunker{}

	c.Feed("...")

	assert.Equal(t, "", c.Flush(), "a content-free remainder has nothing to absorb into, so it's dropped")
}

// TestSentenceChunker_DigitBeforeDotDoesNotCut is the regression test for the
// live TTS rejections: a '.' directly after a digit is a numbered-list marker
// ("1.", "\n2.") or a decimal point, not a sentence end. The digit counts as
// content, so without this rule each marker flushed as its own digit-only
// chunk, which Sarvam's TTS rejects with a 400 ("must contain at least one
// character from the allowed languages") -- silently skipping a request
// mid-reply.
func TestSentenceChunker_DigitBeforeDotDoesNotCut(t *testing.T) {
	cases := []struct {
		name  string
		feeds []string
		want  []string
	}{
		{
			name:  "numbered list marker absorbs into its sentence",
			feeds: []string{"Steps: ", "1.", " Do the thing."},
			want:  []string{"Steps: 1. Do the thing."},
		},
		{
			name:  "marker arriving token by token still absorbs",
			feeds: []string{"Hello there. ", "\n", "2.", " Second item."},
			want:  []string{"Hello there.", " \n2. Second item."},
		},
		{
			name:  "decimal point does not split the sentence",
			feeds: []string{"It costs 3.5 units."},
			want:  []string{"It costs 3.5 units."},
		},
		{
			name:  "sentence genuinely ending in a number merges into the next",
			feeds: []string{"He was born in 1990. He moved later."},
			want:  []string{"He was born in 1990. He moved later."},
		},
		{
			name:  "real sentences still cut normally after a digit sentence",
			feeds: []string{"In 1990. Then what?"},
			want:  []string{"In 1990. Then what?"},
		},
		{
			name:  "danda is unaffected by the digit rule",
			feeds: []string{"संख्या 5। अगला।"},
			want:  []string{"संख्या 5।", " अगला।"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &SentenceChunker{}

			var out []string

			for _, tok := range tc.feeds {
				out = append(out, c.Feed(tok)...)
			}

			if rest := c.Flush(); rest != "" {
				out = append(out, rest)
			}

			assert.Equal(t, tc.want, out)
		})
	}
}
