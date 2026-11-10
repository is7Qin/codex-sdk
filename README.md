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
- **Fingerprint alignment**: headers, send-frame top-level key whitelist, and
  `client_metadata` assembly aligned with the real Codex client.
- **Request-scoped identity**: `Identity` carries session/meta/turn-state per
  call; the SDK keeps no identity state and reports issued turn-state only
  through `Identity.Sink`. Identity headers are reserved: `WithHeader` panics at
  construction time if they are used.
- **Protocol-aware transport**: the SDK owns framing, keepalive and the SSE
  representation, and reports attributable transport errors
  (`ErrWSBackpressure` / `ErrKeepaliveTimeout` / `*HTTPError` with an
  `Unwrap`-able cause). Business semantics (types / usage / events) still live
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

`Identity.Session` supplies the WS handshake headers, `Identity.Meta` supplies
the frame/body `client_metadata`, `Identity.TurnState` is the per-request
turn-state, and `Identity.Sink` receives any turn-state issued by upstream.
Because `x-codex-turn-state` and the other identity headers are reserved,
`WithHeader` panics at construction time when they are used.

For the full API reference, see the package documentation in
[doc.go](./doc.go).

## License

Dual-licensed under [LGPL-3.0](./LICENSE) (open source) or a commercial
license ([LICENSE.commercial](./LICENSE.commercial)) for closed-source
distribution of modified versions. Copyright (c) 2026 is7Qin.
