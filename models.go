package codexsdk

import (
	_ "embed"
	"encoding/json"
)

// modelsLimitsJSON 是 codex 模型目录的裁剪副本（只留上下文窗口相关字段），
// 来源 openai/codex codex-rs/models-manager/models.json（见文件内 _source /
// _codex_commit）。上游更新时同步这个文件即可。
//
//go:embed models_limits.json
var modelsLimitsJSON []byte

// ModelLimits 是模型的上下文窗口信息（codex ModelInfo 的最小投影）。
type ModelLimits struct {
	ContextWindow    int64
	MaxContextWindow int64
	// AutoCompactTokenLimit 对应 codex 的 auto_compact_token_limit（可为 nil）。
	AutoCompactTokenLimit *int64
}

// unknownModelLimits 未知 slug 的 fallback，对齐 codex
// models-manager/src/model_info.rs model_info_from_slug（272000 / 272000）。
var unknownModelLimits = ModelLimits{ContextWindow: 272000, MaxContextWindow: 272000}

type modelLimitsEntry struct {
	Slug                  string `json:"slug"`
	ContextWindow         int64  `json:"context_window"`
	MaxContextWindow      int64  `json:"max_context_window"`
	AutoCompactTokenLimit *int64 `json:"auto_compact_token_limit"`
}

type modelLimitsFile struct {
	Models []modelLimitsEntry `json:"models"`
}

var modelCatalog = mustLoadModelCatalog()

func mustLoadModelCatalog() map[string]ModelLimits {
	var f modelLimitsFile
	if err := json.Unmarshal(modelsLimitsJSON, &f); err != nil {
		// 内嵌常量解析失败属编译期错误：panic 优于静默返回空目录。
		panic("codexsdk: 模型目录解析失败: " + err.Error())
	}
	m := make(map[string]ModelLimits, len(f.Models))
	for _, e := range f.Models {
		m[e.Slug] = ModelLimits{
			ContextWindow:         e.ContextWindow,
			MaxContextWindow:      e.MaxContextWindow,
			AutoCompactTokenLimit: e.AutoCompactTokenLimit,
		}
	}
	return m
}

// ModelLimitsFor 返回 slug 的上下文窗口；未知 slug → fallback（272000）。
func ModelLimitsFor(slug string) ModelLimits {
	if l, ok := modelCatalog[slug]; ok {
		return l
	}
	return unknownModelLimits
}

// ResolvedContextWindow 是 codex 的解析口径：context_window 优先，缺失才回退
// max_context_window（protocol/src/openai_models.rs resolved_context_window）。
func ResolvedContextWindow(slug string) int64 {
	l := ModelLimitsFor(slug)
	if l.ContextWindow > 0 {
		return l.ContextWindow
	}
	return l.MaxContextWindow
}

// AutoCompactTokens 是 codex 的自动压缩阈值 θ_w：
//
//	θ_w = min(auto_compact_token_limit, resolved_context_window × 9/10)
//
// （protocol/src/openai_models.rs auto_compact_token_limit）。目录内该字段当前
// 全为 null，故实际等于 resolved × 9/10；上游若收紧该值会自动生效。
func AutoCompactTokens(slug string) int64 {
	l := ModelLimitsFor(slug)
	// 就地算 resolved_context_window（context_window 优先，缺失回退
	// max_context_window），省去 ResolvedContextWindow 的二次查表。
	resolved := l.ContextWindow
	if resolved <= 0 {
		resolved = l.MaxContextWindow
	}
	contextLimit := resolved * 9 / 10
	if l.AutoCompactTokenLimit != nil && *l.AutoCompactTokenLimit < contextLimit {
		return *l.AutoCompactTokenLimit
	}
	return contextLimit
}
