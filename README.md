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
- **Search**: `HTTPClient.Search` against the `/alpha/search` endpoint.
- **Unified client**: all endpoints go through `HTTPClient` methods
  (`Do` / `Stream` / `Responses` / `GenerateImage` / `Search`), with
  endpoints derived from the base URL.
- **Authentication**: static PAT, OAuth with refresh callback, and
  `OAuthWithRotation` (rotating state machine with single-flight refresh,
  fatal-account detection, and 401 auto-rotation).
- **Fingerprint alignment**: headers, send-frame top-level key whitelist, and
  `client_metadata` assembly aligned with the real Codex client.
- **Zero protocol parsing**: the SDK delivers complete raw bytes; type/usage/
  event semantics and business logic live in the gateway
  ([go-proxy-mini](https://github.com/is7Qin/go-proxy-mini)).

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

	// Search (endpoint derived from the base URL).
	payload := []byte(`{"query":"example"}`)
	resp, err := client.Search(ctx, payload)
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.StatusCode, string(resp.Raw))
}
```

For the full API reference, see the package documentation in
[doc.go](./doc.go).

## License

Dual-licensed under [LGPL-3.0](./LICENSE) (open source) or a commercial
license ([LICENSE.commercial](./LICENSE.commercial)) for closed-source
distribution of modified versions. Copyright (c) 2026 is7Qin.
