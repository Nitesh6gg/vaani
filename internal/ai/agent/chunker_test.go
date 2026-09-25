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
