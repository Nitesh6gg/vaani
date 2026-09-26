// Package agent implements Phase 3's media.Handler: Sarvam STT -> OpenAI-compatible
// LLM -> Sarvam TTS, with local barge-in detection. See docs/AI_PROVIDERS.md.
package agent

import (
	"unicode"
	"unicode/utf8"
)

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
//
// A delimiter only cuts once the segment has at least one letter/digit before
// it -- otherwise a run of bare punctuation (e.g. "..." arriving token by
// token, or a single "." left over from a decimal number) would each flush as
// its own "sentence": wasted, since Sarvam's TTS rejects content-free text
// with a 400 ("must contain at least one character from the allowed
// languages", observed live), and every such request is also a needless
// network round trip that can stall audio delivery mid-turn. Withholding the
// cut lets the punctuation absorb into the next real content instead.
func (c *SentenceChunker) nextCut() (cut int, ok bool) {
	runeCount := 0
	hasContent := false

	for i := 0; i < len(c.buf); {
		r, size := utf8.DecodeRune(c.buf[i:])
		i += size
		runeCount++

		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			hasContent = true
		}

		if runeCount >= chunkerRuneThreshold || (sentenceDelimiters[r] && hasContent) {
			return i, true
		}
	}

	return 0, false
}

// Flush returns any remaining buffered text -- call at the end of an LLM turn
// so a trailing fragment with no delimiter isn't lost. Returns "" if the
// remainder has no letter/digit content (see nextCut's doc comment) -- there's
// no later token for bare trailing punctuation to absorb into, so it's
// dropped rather than sent as its own doomed request.
func (c *SentenceChunker) Flush() string {
	if len(c.buf) == 0 {
		return ""
	}

	s := string(c.buf)
	c.buf = nil

	if !hasLetterOrDigit(s) {
		return ""
	}

	return s
}

func hasLetterOrDigit(s string) bool {
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return true
		}
	}

	return false
}
