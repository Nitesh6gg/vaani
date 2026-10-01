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
// ~27 characters). Past it, the chunk ends at the last word break, never
// inside a word: each chunk is a separate Sarvam request, spoken on its own,
// so a word split across two (observed live: "बिजल" + "ी", "संतुष्" + "ट")
// is heard broken in half.
const chunkerRuneThreshold = 80

// chunkerHardLimit is where a chunk with no word break at all (one enormous
// "word") is cut anyway -- still never before a combining mark (a vowel sign)
// or right after a virama, which would split a written syllable.
const chunkerHardLimit = 2 * chunkerRuneThreshold

// virama (U+094D) joins the consonants around it into one conjunct.
const virama = '्'

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
//
// A '.' directly after a digit doesn't cut either, even though the digit
// counts as content: it's a numbered-list marker ("1.", "\n2." -- observed
// live: each marker flushed as its own digit-only chunk and was rejected by
// Sarvam with that same 400, silently skipping a request mid-reply) or a
// decimal point ("3.5" splitting into "3." + "5…"). Only '.' is special-cased
// -- danda, !, ?, and newline don't occur inside numbers or list markers.
// Tradeoff: an English sentence genuinely ending in a number ("born in
// 1990.") merges into the following sentence and starts slightly later.
//
// Past chunkerRuneThreshold with no delimiter, the cut goes after the last
// space that follows a comma/semicolon/colon (a natural pause), else after
// the last space -- so the chunk ends with that space and the next one starts
// on a whole word.
func (c *SentenceChunker) nextCut() (cut int, ok bool) {
	runeCount := 0
	hasContent := false
	lastSpace, lastPause := 0, 0

	var prev rune

	for i := 0; i < len(c.buf); {
		r, size := utf8.DecodeRune(c.buf[i:])
		i += size
		runeCount++

		if unicode.IsSpace(r) && hasContent {
			lastSpace = i

			// A pause only counts in the chunk's second half: an early comma
			// would just make a needlessly short request.
			if (prev == ',' || prev == ';' || prev == ':') && runeCount >= chunkerRuneThreshold/2 {
				lastPause = i
			}
		}

		if runeCount >= chunkerRuneThreshold && !(sentenceDelimiters[r] && hasContent) {
			switch {
			case lastPause > 0:
				return lastPause, true
			case lastSpace > 0:
				return lastSpace, true
			case runeCount >= chunkerHardLimit && i < len(c.buf):
				if next, _ := utf8.DecodeRune(c.buf[i:]); !unicode.Is(unicode.M, next) && r != virama {
					return i, true
				}
			}
		}

		if sentenceDelimiters[r] && hasContent {
			if r == '.' && unicode.IsDigit(prev) {
				prev = r
				continue
			}

			return i, true
		}

		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			hasContent = true
		}

		prev = r
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
