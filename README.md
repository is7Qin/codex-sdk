# codex-sdk

A Go client library for the OpenAI Responses protocol targeting the Codex
upstream, in both WebSocket and HTTP forms, exposed through a single raw-byte
channel API.

## Features

- **Responses WebSocket**: dial, raw frame send/receive (text and binary
  passthrough), heartbeat keepalive, close-code passthrough.
- **Responses HTTP**: request construction, non-streaming responses, streaming
  SSE event-frame extraction.
- **Image generation**: `GenerateImage` (non-streaming, direct images endpoint)
  and `GenerateImageStream` (synthesized streaming with keepalive and
  `image_generation.completed` events).
- **Search**: `HTTPClient.Search` against the fixed official `/alpha/search` endpoint.
- **Unified client**: all endpoints go through `HTTPClient` methods
  (`Do` / `Stream` / `Responses` / `GenerateImage` / `Search`), with
  fixed official endpoints (no base URL configuration).
- **Authentication**: static PAT, OAuth with refresh callback, and
  `OAuthWithRotation` (rotating state machine with single-flight refresh,
  fatal-account detection, and 401 auto-rotation).
- **Device-code login** (`DeviceAuthClient`): ChatGPT OAuth device
  authorization grant — `StartDeviceFlow` (user code, verification URL, always
  positive expiry) and a single-shot `PollDeviceFlow` that either reports
  pending/expired/denied through sentinel errors or exchanges the code and
  returns tokens. Stateless by design: the caller keeps the pending session, and
  there is no blocking loop in the library.
- **Request shaping (codex-tui 0.169.0)**: every Responses request
  (HTTP→HTTP, HTTP→WS, WS→WS) is shaped in two stages — a single top-level
  parse (whitelist, first-occurrence wins for duplicate keys) and an
  identity/model-aware shaping step — then written in the official key order.
  Official headers (`version`, `x-codex-beta-features`, `x-codex-routing-hint`,
  identity headers, `x-codex-turn-metadata`, lower-case names), zstd request
  bodies on `POST /responses`, `instructions` → developer message (official
  UUIDv5 id), responses-lite `additional_tools` conversion from a static model
  table, randomized `client_metadata` key order, ASCII turn-metadata, and
  `permessage-deflate` negotiation on the WS handshake (outbound frames are not
  compressed; `WithCompression` only affects negotiation). Invalid bodies
  (non-object root, missing/empty `model` or one with control bytes, control
  bytes in `service_tier`) fail locally with `ErrInvalidRequestBody` — no
  request is sent and no dial happens. Request bodies must already be validated
  by a depth-limited validator such as `encoding/json`'s `json.Valid`.
  Counters: `ShapeStatsSnapshot()`. `HTTPClient.Do` is **not** shaped and must
  not be used for Codex responses.
- **Request-scoped identity**: `Identity` (`InstallationID`, `Session`,
  `Window`, `Turn`, `Sink`) is passed per call; the SDK keeps no identity state
  and reports issued turn-state only through `Identity.Sink` (HTTP response
  headers and `response.metadata` events; never WS handshake headers). Identity
  and shape headers are reserved: `WithHeader` panics at construction time if
  they are used.
- **Protocol-aware transport**: the SDK owns framing and the SSE representation
  (no active heartbeat — aligned with the real Codex client, which only answers
  pings), and reports attributable transport errors
  (`ErrWSBackpressure` / `*HTTPError` with an `Unwrap`-able cause). Business
  semantics (types / usage / events) still live
  in the gateway ([go-proxy-mini](https://github.com/is7Qin/go-proxy-mini)).

## Installation

```sh
go get github.com/is7Qin/codex-sdk
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"

	codexsdk "github.com/is7Qin/codex-sdk"
)

func main() {
	ctx := context.Background()

	// OAuth with a rotation state machine (recommended).
	auth := codexsdk.OAuthWithRotation("refresh-token",
		codexsdk.WithOnTokenRotated(func(at, rt string) {
			// Persist the rotated tokens.
		}),
	)

	client := codexsdk.NewHTTPClient(auth)

	// Search (fixed official endpoint).
	payload := []byte(`{"query":"example"}`)
	resp, err := client.Search(ctx, payload)
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.StatusCode, string(resp.Raw))
}
```

## Identity is request data (breaking change)

`Stream`, `StreamBody`, `Responses` and `Dial` take an `Identity` argument.
The old identity options (`WithSession`, `WithCodexMeta`,
`WithTurnStateProvider`, `WithTurnStateObserver`, `WithClientMetadata`) and the
turn-state getters/setters (`HTTPClient.TurnState`, `HTTPResponse.TurnState`,
`Client.TurnState`/`SetTurnState`) are removed.

`Identity` is `{InstallationID, Session, Window, Turn, Sink}`. `Session`
supplies the session/thread/window ids, `Window` the window number and
`context_window_id`, `Turn` the turn id, start time and the turn-state to send
(`Turn.State`), and `Sink` receives any turn-state issued by upstream. The
`client_metadata` and `x-codex-turn-metadata` are synthesized entirely from the
identity (whatever the client sent is dropped). A zero `Session.SessionID`
means anonymous: no identity headers, no `client_metadata`, no turn-metadata,
no `prompt_cache_key`. `CodexMeta`, `Identity.Meta`, `Identity.TurnState`,
`WithTurnMetadata` and `WithTurnAuto` are removed; `WithTraceAuto` now
defaults to `false`. Slot-pool callers use `IdentityState.Identity(sink, first)`
with `first = !AnalyzeConversationInput(payload).FollowUp()`.

For WS→WS (`Dial`), pass the first frame with `WithFirstFrame(frame)` so the
handshake carries `x-codex-routing-hint` and the first frame's
`x-codex-turn-metadata`. The SDK does not copy `frame`: it must not be modified
before the first `Send`, and is released after it. On a WS→WS connection,
`instructions`/`tools` items are inserted once: an incremental frame (with
`previous_response_id`) whose inserted items equal the connection's previous
insertion only has the top-level fields removed. Callers should give the SDK a transport with
`DisableCompression` (official clients send no `Accept-Encoding`).

For the full API reference, see the package documentation in
[doc.go](./doc.go).

## License

Dual-licensed under [LGPL-3.0](./LICENSE) (open source) or a commercial
license ([LICENSE.commercial](./LICENSE.commercial)) for closed-source
distribution of modified versions. Copyright (c) 2026 is7Qin.
