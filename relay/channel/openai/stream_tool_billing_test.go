package openai

import (
	"net/http"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOaiStreamTruncatedEOFDoesNotInventSettlement(t *testing.T) {
	oldTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 30
	t.Cleanup(func() { constant.StreamingTimeout = oldTimeout })
	body := "data: " + `{"id":"partial","object":"chat.completion.chunk","model":"gpt-test","choices":[{"index":0,"delta":{"content":"partial answer"},"finish_reason":null}]}` + "\n\n"
	c, recorder, resp, info := newResponsesChatTestContext(t, body, true)
	info.RelayMode = relayconstant.RelayModeChatCompletions
	info.SetEstimatePromptTokens(12)
	usage, apiErr := OaiStreamHandler(c, info, resp)
	require.NotNil(t, apiErr, "clean HTTP EOF is not a completed model response")
	assert.Nil(t, usage, "partial text must not become a final estimated charge")
	assert.Equal(t, http.StatusBadGateway, apiErr.StatusCode)
	assert.Equal(t, types.ErrorCodeBadResponseBody, apiErr.GetErrorCode())
	assert.True(t, types.IsSkipRetryError(apiErr), "a partial response must not be replayed upstream")
	assert.True(t, service.BillingFailureOutcomeUnknown(apiErr), "retain the durable reservation for verification")
	assert.False(t, strings.Contains(recorder.Body.String(), "[DONE]"))
	assert.NotContains(t, recorder.Body.String(), `"usage"`)
	assert.Equal(t, "incomplete", info.StreamStatus.ResponseOutcome())
}

func TestOaiStreamKnownCompletionAndUsageRemainBillable(t *testing.T) {
	oldTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 30
	t.Cleanup(func() { constant.StreamingTimeout = oldTimeout })
	partial := `{"id":"chat-1","object":"chat.completion.chunk","model":"gpt-test","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":null}]}`
	finish := `{"id":"chat-1","object":"chat.completion.chunk","model":"gpt-test","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`
	usageFrame := `{"id":"chat-1","object":"chat.completion.chunk","model":"gpt-test","choices":[],"usage":{"prompt_tokens":12,"completion_tokens":8,"total_tokens":20}}`
	for _, tc := range []struct {
		name, suffix string
		estimated    bool
	}{
		{"finish_then_eof", "data: " + finish + "\n\n", true},
		{"explicit_done", "data: [DONE]\n\n", true},
		{"provider_usage_then_eof", "data: " + usageFrame + "\n\n", false},
		{"finish_usage_done", "data: " + finish + "\n\ndata: " + usageFrame + "\n\ndata: [DONE]\n\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, recorder, resp, info := newResponsesChatTestContext(t, "data: "+partial+"\n\n"+tc.suffix, true)
			info.RelayMode = relayconstant.RelayModeChatCompletions
			info.SetEstimatePromptTokens(12)
			usage, apiErr := OaiStreamHandler(c, info, resp)
			require.Nil(t, apiErr)
			require.NotNil(t, usage)
			assert.Equal(t, 12, usage.PromptTokens)
			if !tc.estimated {
				assert.Equal(t, 8, usage.CompletionTokens)
				assert.Equal(t, 20, usage.TotalTokens)
			}
			assert.Equal(t, tc.estimated, common.GetContextKeyBool(c, constant.ContextKeyLocalCountTokens))
			assert.Contains(t, recorder.Body.String(), "[DONE]")
		})
	}
}

func TestObserveStreamChoicesDedupesFunctionCallNames(t *testing.T) {
	info := &relaycommon.RelayInfo{StreamStatus: relaycommon.NewStreamStatus()}
	seen := make(map[string]struct{})
	var names []string

	chunks := []string{
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","type":"function","function":{"name":"get_weather","arguments":""}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"q\":"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"x\"}"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"c2","type":"function","function":{"name":"get_time","arguments":""}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"arguments":"{}"}}]}}]}`,
	}
	for _, chunk := range chunks {
		observeStreamChoices(info, chunk, seen, &names)
	}

	require.Len(t, names, 2)
	assert.Equal(t, []string{"get_weather", "get_time"}, names)
	assert.Empty(t, info.StreamStatus.ResponseOutcome(), "tool call deltas carry no finish reason")

	observeStreamChoices(info, `{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`, seen, &names)
	assert.Equal(t, "completed", info.StreamStatus.ResponseOutcome())
	assert.False(t, info.PerformanceBusinessRejection)

	filtered := &relaycommon.RelayInfo{StreamStatus: relaycommon.NewStreamStatus()}
	observeStreamChoices(filtered, `{"choices":[{"index":0,"delta":{},"finish_reason":"content_filter"}]}`, map[string]struct{}{}, &names)
	assert.True(t, filtered.PerformanceBusinessRejection)
}
