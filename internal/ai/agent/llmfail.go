package agent

import "strings"

// LLMFailurePolicy is what the agent says when the LLM fails a turn.
type LLMFailurePolicy struct {
	// MaxFailedTurns consecutive failed turns end the call (0 = never).
	MaxFailedTurns int
	// Apology replaces the default line asking the caller to repeat.
	Apology string
	// Language is the TTS language ("hi-IN"), choosing the default lines.
	Language string
}

// line is what to say after the n-th consecutive failed turn, its Sink kind,
// and whether the call then ends.
func (p LLMFailurePolicy) line(n int) (text, kind string, hangup bool) {
	hindi := strings.HasPrefix(strings.ToLower(p.Language), "hi")

	if p.MaxFailedTurns > 0 && n >= p.MaxFailedTurns {
		if hindi {
			return "माफ़ कीजिए, कुछ तकनीकी समस्या है। हम आपको थोड़ी देर में दोबारा कॉल करेंगे। धन्यवाद।", "hangup", true
		}

		return "Sorry, we are having a technical problem. We will call you back shortly. Thank you.", "hangup", true
	}

	switch {
	case p.Apology != "":
		return p.Apology, "apology", false
	case hindi:
		return "माफ़ कीजिए, मुझे ठीक से सुनाई नहीं दिया। क्या आप दोबारा बोल सकते हैं?", "apology", false
	}

	return "Sorry, I didn't catch that. Could you please say it again?", "apology", false
}
