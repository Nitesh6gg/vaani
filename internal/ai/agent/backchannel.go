package agent

import (
	"log/slog"
	"strings"
	"time"
	"unicode"
)

// backchannelWords are acknowledgements a caller makes while listening --
// "जी", "हाँ", "ok", "hmm" -- in English and the Indian languages Sarvam
// transcribes (Hindi, Bengali, Gujarati, Marathi, Punjabi, Odia, Tamil,
// Telugu, Kannada, Malayalam), plus the Latin spellings code-mixed speech
// comes out in ("haan ji", "achha"). A transcript made only of these, said
// while a reply is paused for a possible interruption, means "go on", not
// "stop": call 509 (2026-10-08) had a lone "जी" cut a question in half, the
// LLM answer "sorry, I didn't understand", and two turns lost recovering.
//
// Deliberately absent: anything that refuses, questions or stops -- नहीं,
// ना, no, wait, क्या, रुको, and their equivalents -- so "जी नहीं" or
// "हाँ लेकिन..." still interrupt (one word outside this list is enough).
// Multi-word acknowledgements are covered word by word ("ठीक है" = ठीक +
// है), which is why a few bare words like है, आहे, আছে are here: alone or
// with each other they acknowledge, next to anything else they don't decide.
var backchannelWords = setOf(
	// English, and Latin-script Hindi/Hinglish as Sarvam writes it.
	"yes", "yeah", "yea", "yep", "yup", "ok", "okay", "okk", "k", "alright", "right", "sure",
	"hmm", "hm", "mm", "mhm", "mmhmm", "uh", "huh", "uhhuh", "aha", "ah", "oh",
	"haan", "han", "haa", "ha", "haanji", "hanji", "ji", "jee",
	"achha", "acha", "accha", "achchha", "theek", "thik", "theekhai", "hai", "sahi", "bilkul",

	// Hindi / Urdu (Devanagari).
	"जी", "हाँ", "हां", "हा", "हाँजी", "हांजी", "जीहाँ", "हम्म", "हम्म्म", "हम", "हं", "हूँ", "हुँ", "ऊँ", "उँ",
	"अच्छा", "अच्छाजी", "ठीक", "है", "सही", "बिल्कुल", "बिलकुल", "ओके", "ओह", "आह",
	// Urdu (Arabic script): جی ہاں اچھا ٹھیک ہے
	"جی", "ہاں", "اچھا", "ٹھیک", "ہے",
	// Marathi.
	"हो", "होय", "बरं", "बर", "आहे",
	// Gujarati.
	"હા", "હાં", "જી", "હમ્મ", "બરાબર", "સારું", "ઓકે", "અચ્છા", "ઠીક", "છે",
	// Bengali.
	"হ্যাঁ", "হ্যা", "হুম", "আচ্ছা", "ঠিক", "আছে", "ওকে", "জি",
	// Punjabi (Gurmukhi).
	"ਹਾਂ", "ਹਾਂਜੀ", "ਜੀ", "ਹਮ", "ਅੱਛਾ", "ਠੀਕ", "ਹੈ", "ਓਕੇ",
	// Odia.
	"ହଁ", "ହଉ", "ଆଚ୍ଛା", "ଠିକ୍", "ଠିକ", "ଅଛି", "ଓକେ",
	// Tamil.
	"ஆமா", "ஆமாம்", "சரி", "ம்ம்", "ம்", "ஹ்ம்", "ஓகே", "ஆ",
	// Telugu.
	"అవును", "సరే", "అలాగే", "హా", "ఊ", "హ్మ్", "ఓకే",
	// Kannada.
	"ಹೌದು", "ಸರಿ", "ಹಾ", "ಹ್ಮ್", "ಓಕೆ", "ಆಯ್ತು",
	// Malayalam.
	"അതെ", "ശരി", "ഉം", "ഹാ", "ഓക്കെ", "ഓകെ",
)

// heldSpeech is something the caller said over a reply that isn't (yet) a
// turn (see Config.AckFilter): gen is the reply it was said over, at when
// the caller began saying it.
type heldSpeech struct {
	text string
	gen  uint64
	at   time.Time
}

// holdSpeech keeps what the caller said over the agent until the reply
// finishes, instead of dropping it: an acknowledgement over a paused reply,
// or anything said over a reply at an interruptible node that was too short
// to pause it -- a quick "हाँ" over the end of a question (call 516,
// 2026-10-09: three such answers lost). Like LiveKit's held transcripts --
// "interrupting the agent is recoverable, discarding a real user turn is
// not" -- but decided by sentence, not a fixed second (settleHeld).
func (h *Handler) holdSpeech(text, why string) {
	at := h.utteranceStart()
	if at.IsZero() {
		at = time.Now() // unknown start: the benefit of the doubt goes to "an answer"
	}

	h.held = append(h.held, heldSpeech{text: text, gen: h.curGen, at: at})
	h.cfg.Log.SpeechHeld()

	slog.Info("caller speech held until the reply ends", "call_id", h.callID, "text", text,
		"gen", h.curGen, "why", why, "event", "agent.speech.held")
}

// dropHeld discards everything held, saying why.
func (h *Handler) dropHeld(why string) {
	for _, s := range h.held {
		h.logDropped(s, why)
	}

	h.held = nil
}

func (h *Handler) logDropped(s heldSpeech, why string) {
	slog.Info("held caller speech dropped", "call_id", h.callID, "text", s.text,
		"why", why, "event", "agent.speech.dropped")
}

// settleHeld decides what was held once reply gen has fully played: what
// the caller said during its last sentence (from AckEndMargin before that
// sentence began) answers what the agent was finishing -- "...क्या आप
// संतुष्ट हैं?" "हाँ" -- and becomes their turn now, joined in the order said
// (true: a turn started); what they said earlier was listening, and is
// dropped.
func (h *Handler) settleHeld(gen uint64) bool {
	held := h.held
	h.held = nil

	from := time.Unix(0, h.playingSince.Load()).Add(-h.cfg.AckEndMargin)

	var (
		answer []string
		at     time.Time
	)

	for _, s := range held {
		switch {
		case s.gen != gen:
			h.logDropped(s, "said over an earlier reply")
		case s.at.Before(from):
			h.logDropped(s, "said before the reply's last sentence")
		default:
			if at.IsZero() {
				at = s.at
			}

			answer = append(answer, s.text)
		}
	}

	if len(answer) == 0 {
		return false
	}

	text := strings.Join(answer, " ")
	h.cfg.Log.SpeechDelivered()

	slog.Info("held caller speech delivered as their turn", "call_id", h.callID, "text", text,
		"said_before_end_ms", time.Since(at).Milliseconds(), "event", "agent.speech.delivered")
	h.acceptUserTurn(text, at, time.Time{}, time.Time{}, time.Time{})

	return true
}
func setOf(words ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(words))
	for _, w := range words {
		m[w] = struct{}{}
	}

	return m
}

// backchannel reports whether text is nothing but acknowledgement words
// (backchannelWords), in any mix of the languages above. Punctuation --
// the danda included -- separates words, Latin is case-insensitive, and a
// letter held long ("hmmmm", "okkkk") counts as its short form.
func backchannel(text string) bool {
	words := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r)
	})
	if len(words) == 0 {
		return false
	}

	for _, w := range words {
		if _, ok := backchannelWords[squeezeRepeats(w)]; !ok {
			return false
		}
	}

	return true
}

// squeezeRepeats cuts any run of one letter to at most two ("hmmmm" ->
// "hmm", "okkkk" -> "okk"), so a drawn-out acknowledgement matches the
// list's short forms.
func squeezeRepeats(w string) string {
	var (
		b    strings.Builder
		prev rune
		run  int
	)

	for _, r := range w {
		if r == prev {
			run++
		} else {
			prev, run = r, 1
		}

		if run <= 2 {
			b.WriteRune(r)
		}
	}

	return b.String()
}
