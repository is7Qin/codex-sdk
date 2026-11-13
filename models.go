package codexsdk

// modelInfo 是请求整形用到的模型目录字段（逐项取自官方 rust-v0.169.0
// codex-rs/models-manager/models.json，与 0.169.0 二进制内置目录一致）。
//
// SDK 只用这张静态表，不跟随线上 /models 目录：表外模型全零值（非 lite、
// 不补 verbosity / effort），并计入 ShapeStats.UnknownModels。
type modelInfo struct {
	Lite                       bool   // use_responses_lite
	NodeReplAutoReviewRequired bool   // node_repl_auto_review_required（turn-metadata）
	NodeReplDisabled           bool   // node_repl_disabled（turn-metadata）
	SupportVerbosity           bool   // support_verbosity
	DefaultVerbosity           string // default_verbosity
	DefaultReasoningLevel      string // default_reasoning_level
}

// modelTable 是 0.169.0 内置的 12 个模型（gpt-5.5 是唯一非 lite）。只读，勿改；
// 更新须同步 TestModelTableMatchesOfficial0169 与 officialsource 比对测试。
var modelTable = map[string]modelInfo{
	"gpt-6-astra":              {Lite: true, NodeReplAutoReviewRequired: true, SupportVerbosity: true, DefaultVerbosity: "low", DefaultReasoningLevel: "low"},
	"gpt-6.1-sol":              {Lite: true, NodeReplAutoReviewRequired: true, SupportVerbosity: true, DefaultVerbosity: "low", DefaultReasoningLevel: "low"},
	"gpt-6.1-terra":            {Lite: true, NodeReplAutoReviewRequired: true, SupportVerbosity: true, DefaultVerbosity: "low", DefaultReasoningLevel: "low"},
	"gpt-6-sol":                {Lite: true, NodeReplAutoReviewRequired: true, SupportVerbosity: true, DefaultVerbosity: "low", DefaultReasoningLevel: "medium"},
	"gpt-6-luna":               {Lite: true, SupportVerbosity: true, DefaultVerbosity: "low", DefaultReasoningLevel: "medium"},
	"gpt-5.6-sol":              {Lite: true, SupportVerbosity: true, DefaultVerbosity: "low", DefaultReasoningLevel: "low"},
	"gpt-5.6-terra":            {Lite: true, SupportVerbosity: true, DefaultVerbosity: "low", DefaultReasoningLevel: "medium"},
	"gpt-5.6-luna":             {Lite: true, SupportVerbosity: true, DefaultVerbosity: "low", DefaultReasoningLevel: "medium"},
	"gpt-daybreak-blue-latest": {Lite: true, SupportVerbosity: true, DefaultVerbosity: "low", DefaultReasoningLevel: "low"},
	"gpt-daybreak-red-latest":  {Lite: true, SupportVerbosity: true, DefaultVerbosity: "high", DefaultReasoningLevel: "medium"},
	"gpt-5.5":                  {SupportVerbosity: true, DefaultVerbosity: "low", DefaultReasoningLevel: "medium"},
	"codex-auto-review":        {Lite: true, SupportVerbosity: true, DefaultVerbosity: "low", DefaultReasoningLevel: "medium"},
}

// lookupModel 查表；ok=false 表示表外模型（全零值）。
func lookupModel(model string) (modelInfo, bool) {
	m, ok := modelTable[model]
	return m, ok
}
