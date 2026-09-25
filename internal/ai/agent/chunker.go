// Package agent implements Phase 3's media.Handler: Sarvam STT -> OpenAI-compatible
// LLM -> Sarvam TTS, with local barge-in detection. See docs/AI_PROVIDERS.md.
package agent

import "unicode/utf8"

// chunkerRuneThreshold bounds how long the SentenceChunker waits for a sentence
// delimiter before flushing anyway, in runes (not bytes -- a byte count badly
// undercounts multi-byte scripts like Devanagari, where 80 bytes is only
// ~27 characters).
const chunkerRuneThreshold = 80

// sentenceDelimiters are flush triggers: standard sentence-ending punctuation
// plus the Devanagari danda and double danda, since Hindi/Marathi/Sanskrit use
// those instead of a period.
var sentenceDelimiters = map[rune]bool{
	'.':  true,
	'!':  true,
	'?':  true,
	'\n': true,
	'।':  true, // U+0964 danda
	'॥':  true, // U+0965 double danda
}

// SentenceChunker buffers streamed LLM tokens and releases complete chunks for
// TTS as soon as either a sentence delimiter appears or the buffer reaches
// chunkerRuneThreshold runes -- whichever comes first -- so audio playback can
// start before the whole LLM response has been generated. Not safe for
// concurrent use; one instance per call turn.
type SentenceChunker struct {
	buf []byte
}

// Feed appends token to the buffer and returns zero or more complete chunks
// ready to hand to TTS, in order.
func (c *SentenceChunker) Feed(token string) []string {
	c.buf = append(c.buf, token...)

	var out []string

	for {
		cut, ok := c.nextCut()
		if !ok {
			break
		}

		out = append(out, string(c.buf[:cut]))
		c.buf = c.buf[cut:]
	}

	return out
}

// nextCut scans the buffered bytes rune by rune (no full-buffer string copy
// per call) and returns the byte offset to flush at, if any trigger has fired.
func (c *SentenceChunker) nextCut() (cut int, ok bool) {
	runeCount := 0

	for i := 0; i < len(c.buf); {
		r, size := utf8.DecodeRune(c.buf[i:])
		i += size
		runeCount++

		if sentenceDelimiters[r] || runeCount >= chunkerRuneThreshold {
			return i, true
		}
	}

	return 0, false
}

// Flush returns any remaining buffered text -- call at the end of an LLM turn
// so a trailing fragment with no delimiter isn't lost.
func (c *SentenceChunker) Flush() string {
	if len(c.buf) == 0 {
		return ""
	}

	s := string(c.buf)
	c.buf = nil

	return s
}
