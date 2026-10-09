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

// heldAck is an acknowledgement said while a reply was paused (see
// Config.AckFilter): gen is the reply it was said over, at when the caller
// began saying it.
type heldAck struct {
	text string
	gen  uint64
	at   time.Time
}

// holdAck keeps an acknowledgement until the paused reply finishes (a newer
// one replaces it: the latest is the one that may answer). Like LiveKit's
// held transcripts -- "interrupting the agent is recoverable, discarding a
// real user turn is not" -- but decided by sentence, not a fixed second.
func (h *Handler) holdAck(text string) {
	at := h.utteranceStart()
	if at.IsZero() {
		at = time.Now() // unknown start: the benefit of the doubt goes to "an answer"
	}

	h.heldAck = &heldAck{text: text, gen: h.curGen, at: at}
	h.cfg.Log.AckHeld()

	slog.Info("acknowledgement held; the reply goes on", "call_id", h.callID, "text", text,
		"gen", h.curGen, "event", "agent.ack.held")
}

// dropAck discards a held acknowledgement, saying why.
func (h *Handler) dropAck(why string) {
	if h.heldAck == nil {
		return
	}

	slog.Info("acknowledgement dropped", "call_id", h.callID, "text", h.heldAck.text,
		"why", why, "event", "agent.ack.dropped")
	h.heldAck = nil
}

// settleAck decides a held acknowledgement once reply gen has fully played:
// said during its last sentence (from AckEndMargin before that sentence
// began), it answers what the agent was finishing -- "...क्या आप संतुष्ट
// हैं?" "हाँ" -- and becomes the caller's turn now (true: a turn started);
// said earlier, the caller was only listening, and it's dropped.
func (h *Handler) settleAck(gen uint64) bool {
	a := h.heldAck
	if a == nil {
		return false
	}

	if a.gen != gen {
		h.dropAck("said over an earlier reply")
		return false
	}

	lastSentence := time.Unix(0, h.playingSince.Load())
	if a.at.Before(lastSentence.Add(-h.cfg.AckEndMargin)) {
		h.dropAck("said before the reply's last sentence")
		return false
	}

	h.heldAck = nil
	h.cfg.Log.AckDelivered()

	slog.Info("acknowledgement delivered as the caller's turn", "call_id", h.callID, "text", a.text,
		"said_before_end_ms", time.Since(a.at).Milliseconds(), "event", "agent.ack.delivered")
	h.acceptUserTurn(a.text, a.at, time.Time{}, time.Time{}, time.Time{})

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
