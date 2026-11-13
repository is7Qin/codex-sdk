//go:build officialsource

package codexsdk

import (
	"encoding/json"
	"os"
	"os/exec"
	"testing"
)

// TestModelTableMatchesOfficialSource 与官方源码逐项比对模型表（默认不跑：需要本地
// openai/codex 仓库）。运行：
//
//	CODEX_REPO=/path/to/codex go test -tags officialsource -run TestModelTableMatchesOfficialSource .
func TestModelTableMatchesOfficialSource(t *testing.T) {
	repo := os.Getenv("CODEX_REPO")
	if repo == "" {
		t.Skip("未设置 CODEX_REPO")
	}
	out, err := exec.Command("git", "-C", repo, "show", "rust-v0.169.0:codex-rs/models-manager/models.json").Output()
	if err != nil {
		t.Fatalf("git show: %v", err)
	}
	var doc struct {
		Models []struct {
			Slug                       string `json:"slug"`
			UseResponsesLite           bool   `json:"use_responses_lite"`
			NodeReplAutoReviewRequired bool   `json:"node_repl_auto_review_required"`
			NodeReplDisabled           bool   `json:"node_repl_disabled"`
			SupportVerbosity           bool   `json:"support_verbosity"`
			DefaultVerbosity           string `json:"default_verbosity"`
			DefaultReasoningLevel      string `json:"default_reasoning_level"`
		} `json:"models"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Models) != len(modelTable) {
		t.Fatalf("官方 %d 个模型，表 %d 行", len(doc.Models), len(modelTable))
	}
	for _, m := range doc.Models {
		want := modelInfo{m.UseResponsesLite, m.NodeReplAutoReviewRequired, m.NodeReplDisabled, m.SupportVerbosity, m.DefaultVerbosity, m.DefaultReasoningLevel}
		if got, ok := modelTable[m.Slug]; !ok || got != want {
			t.Fatalf("%s: 表 %+v，官方 %+v", m.Slug, got, want)
		}
	}
}
