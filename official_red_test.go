package codexsdk

import "testing"

func TestRED_OfficialEndpointsFixed(t *testing.T) {
	if DefaultResponsesURL != "https://chatgpt.com/backend-api/codex/responses" {
		t.Fatalf("DefaultResponsesURL = %q", DefaultResponsesURL)
	}
	if DefaultResponsesWSURL != "wss://chatgpt.com/backend-api/codex/responses" {
		t.Fatalf("DefaultResponsesWSURL = %q", DefaultResponsesWSURL)
	}
	if DefaultSearchURL != "https://chatgpt.com/backend-api/codex/alpha/search" {
		t.Fatalf("DefaultSearchURL = %q", DefaultSearchURL)
	}
	if DefaultImagesURL != "https://chatgpt.com/backend-api/codex/images/generations" {
		t.Fatalf("DefaultImagesURL = %q", DefaultImagesURL)
	}
	if DefaultImagesEditsURL != "https://chatgpt.com/backend-api/codex/images/edits" {
		t.Fatalf("DefaultImagesEditsURL = %q", DefaultImagesEditsURL)
	}
	if DefaultUsageURL != "https://chatgpt.com/backend-api/wham/usage" {
		t.Fatalf("DefaultUsageURL = %q", DefaultUsageURL)
	}
}
