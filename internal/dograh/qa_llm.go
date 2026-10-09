package dograh

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/nitesh/vaani/internal/ai/agent"
	"github.com/nitesh/vaani/internal/ai/llm"
)

// nodeSummarySystemPrompt is Dograh's NODE_SUMMARY_SYSTEM_PROMPT
// (qa/node_summary.py), word for word.
const nodeSummarySystemPrompt = "You are analyzing a voice AI agent script. This is only a part of a larger script. " +
	"Produce a concise summary (2-4 sentences) describing this script purpose, " +
	"what the agent should accomplish, and key behaviors. We will be using this " +
	"summary to do a QA on the conversation that the agent would do with someone " +
	"so try to capture the nuances of the script as much as possible."

// conversationSummarySystemPrompt is Dograh's CONVERSATION_SUMMARY_SYSTEM_
// PROMPT (qa/node_summary.py), word for word.
const conversationSummarySystemPrompt = "You are summarizing a portion of a voice AI conversation. " +
	"Produce a concise summary (3-5 sentences) covering key topics, " +
	"information exchanged, and current state. We would be using this " +
	"summary in doing a QA of the conversation that the voice AI agent " +
	"did with someone so try to capture the nuances of the conversation " +
	"as much as possible."

// runOneShot is one request/response LLM call with no tools -- Dograh's
// pipecat-service run_inference, used for node summaries, conversation
// summaries, and the QA review itself (analysis.py's _run_llm_inference).
func runOneShot(ctx context.Context, client *llm.Client, systemPrompt, userContent string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, qaCallTimeout)
	defer cancel()

	var reply strings.Builder

	_, err := client.Stream(ctx, []llm.Message{
		{Role: "system", Content: systemPrompt},
		{Role: "user", Content: userContent},
	}, nil, func(tok string) { reply.WriteString(tok) })
	if err != nil {
		return "", err
	}

	return reply.String(), nil
}

// mustJSON is Dograh's json.dumps(v, indent=2) for a QA prompt's {{metrics}}
// placeholder; "{}" on the (unreachable, since qaMetrics always marshals)
// marshal error rather than a panic from a QA-prompt helper.
func mustJSON(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "{}"
	}

	return string(b)
}

// parseQAReply parses one node's QA LLM reply into Dograh's result shape
// (analysis.py): tags/summary/score/overall_sentiment, all zero-valued if
// the reply wasn't parseable JSON -- never an error, since a bad QA verdict
// isn't a failed call (agent.ParseLLMJSON always returns valid JSON, so
// this never fails to unmarshal either).
func parseQAReply(nodeName, reply string) qaNodeResult {
	raw := agent.ParseLLMJSON(reply)

	var parsed struct {
		Tags             []qaTag  `json:"tags"`
		Summary          string   `json:"summary"`
		Score            *float64 `json:"call_quality_score"`
		OverallSentiment string   `json:"overall_sentiment"`
	}

	_ = json.Unmarshal(raw, &parsed)

	if parsed.Tags == nil {
		parsed.Tags = []qaTag{}
	}

	return qaNodeResult{
		NodeName:         nodeName,
		RawResponse:      reply,
		Tags:             parsed.Tags,
		Summary:          parsed.Summary,
		Score:            parsed.Score,
		OverallSentiment: parsed.OverallSentiment,
	}
}
