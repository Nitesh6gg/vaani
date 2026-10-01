package agent

import (
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	word := "नमस्ते "                        // 7 runes incl. the space
	almostEighty := strings.Repeat(word, 11) // 77 runes, no delimiter

	out := c.Feed(almostEighty)
	assert.Empty(t, out, "must not flush before 80 runes")

	out = c.Feed("अब") // +2 runes = 79
	assert.Empty(t, out)

	out = c.Feed("तक") // 81: past the threshold
	require.Len(t, out, 1, "must flush once the 80-rune threshold is passed")
	assert.Equal(t, almostEighty, out[0], "at the last word break, not mid-word")
	assert.Equal(t, "अबतक", string(c.buf), "the next chunk starts on a whole word")
}

// TestSentenceChunker_NeverSplitsAWord is the regression test for words heard
// broken in half live: past the threshold the cut used to land on exactly
// the 80th rune, splitting "बिजली" into "बिजल" + "ी" (a vowel sign on its own)
// and "संतुष्ट" into "संतुष्" + "ट", each spoken by Sarvam as its own request.
func TestSentenceChunker_NeverSplitsAWord(t *testing.T) {
	// The two live replies whose words were split, without their dandas so
	// the length limit (not a sentence end) decides the cuts.
	cases := []struct {
		text  string
		whole []string // words that were split live
	}{
		{
			text:  "जी बिलकुल, चलिए मुख्य विषय पर बात करते हैं आपके इलाके में सबसे बड़ा चुनावी मुद्दा क्या है - महंगाई, बेरोजगारी, सड़क और बिजली, या कानून व्यवस्था?",
			whole: []string{"बिजली,"},
		},
		{
			text:  "समझ गई अब यह बताइए कि वर्तमान राज्य सरकार के काम से आप कितने संतुष्ट हैं - बहुत संतुष्ट, थोड़े संतुष्ट, या बिल्कुल असंतुष्ट?",
			whole: []string{"संतुष्ट"},
		},
	}

	for _, tc := range cases {
		c := &SentenceChunker{}
		out := c.Feed(tc.text)

		if rest := c.Flush(); rest != "" {
			out = append(out, rest)
		}

		require.Greater(t, len(out), 1, "long enough to be cut")
		assert.Equal(t, tc.text, strings.Join(out, ""), "nothing lost or added")

		words := strings.Fields(tc.text)
		var got []string

		for i, chunk := range out {
			if i < len(out)-1 {
				assert.True(t, strings.HasSuffix(chunk, " "), "every cut is at a word break: %q", chunk)
			}

			assert.LessOrEqual(t, utf8.RuneCountInString(chunk), chunkerHardLimit)
			got = append(got, strings.Fields(chunk)...)
		}

		assert.Equal(t, words, got, "every word arrives whole, in one chunk")

		for _, w := range tc.whole {
			assert.Contains(t, got, w)
		}
	}
}

func TestSentenceChunker_HardLimitNeverSplitsASyllable(t *testing.T) {
	c := &SentenceChunker{}

	// One enormous "word": no space anywhere, so the hard limit applies --
	// but never before a vowel sign or after a virama.
	long := strings.Repeat("क्षि", 50) // क ् ष ि, 4 runes each

	out := c.Feed(long)
	require.NotEmpty(t, out)

	for _, chunk := range out {
		r, _ := utf8.DecodeLastRuneInString(chunk)
		assert.NotEqual(t, virama, r, "a chunk must not end on a virama")

		rest := strings.TrimPrefix(long, chunk)
		if rest != "" {
			next, _ := utf8.DecodeRuneInString(rest)
			assert.False(t, unicode.Is(unicode.M, next), "the next chunk must not start with a vowel sign")
		}

		long = rest
	}
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
