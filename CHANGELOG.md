## 2.0.0

New:

- **Web transport.** `DefaultHttpStreamTransport` now works in browsers via
  `fetch` + `ReadableStream` (only `dart:js_interop`, still zero dependencies),
  with abort on cancel, HTTP status/body failures, back-pressure, CORS notes and
  optional `includeCredentials`. Selected automatically by conditional export.
- **OpenAI.** `OpenAiSseAdapter` now assembles streamed `tool_calls` (parallel
  calls supported), reads usage (`stream_options.include_usage`), finish reasons,
  refusals and in-band errors, and can select a choice via `choiceIndex`. New
  `OpenAiResponsesSseAdapter` covers the Responses API (text, reasoning, function
  calls, citations, usage, completed/incomplete/failed). New
  `CompositeAiAdapter.universal()` routes every built-in protocol.
- **Usage and finish reason.** `AiStreamSnapshot.usage` (`AiStreamUsage`) and
  `finishReason`, populated for Claude, OpenAI Chat and Responses.
- **`IncrementalJsonHealer`.** Scans only newly appended text and caches its
  preview. The client uses it by default (a custom `JsonHealer` still works as
  before). Repair is better: dangling/unfinished keys are dropped, partial
  literals (`tru`) are completed, number suffixes (`1.`, `-`) are trimmed, and
  partial `\uXXXX` escapes are removed.
- **`TimerCoalescer`** replaces the misleadingly named `FrameSyncedCoalescer`
  (kept as a deprecated typedef) and accepts a `frameScheduler`, so Flutter apps
  can truly align updates to frames; `ResilientAiStreamClient(frameScheduler:)`.
- **Localizable status text.** `AiStreamMessages` (`english` default, `arabic`
  preset, or your own) replaces hard-coded Arabic strings.

Fixed:

- A connection that closes in the middle of a multi-byte character (for
  example half of an Arabic letter) is now a `NetworkStreamFailure` instead of
  a `ProtocolParseStreamFailure`, so `restart`/`resume` recovery retries it.
  Invalid UTF-8 *inside* the stream is still a protocol failure. Verified
  against the live Claude API by cutting the stream mid-character.

Breaking changes (hence 2.0.0):

- `AiStreamEvent` gains `MessageMetadataEvent` and `ToolCallsFinishedEvent`;
  exhaustive switches must handle them.
- The default tool-call status text is English; pass `messages:
  AiStreamMessages.arabic` for the old wording. `ClaudeSseAdapter` no longer
  emits a `StatusUpdateEvent` with fixed Arabic text at `tool_use` start.
- `OpenAiSseAdapter` now emits `MessageStartedEvent` for the role chunk, so the
  state becomes `AiStreamThinking` before the first token.
- Healed previews no longer substitute `null` for a key without a value
  (`{"a":` previews as `{}`).
- Earlier unreleased changes: no automatic request replay by default, explicit
  none/restart/resume recovery, EOF without completion is a failure, opt-in
  JSON lines, opening/idle deadlines and per-request cancellation, conditional
  native exports, regression and local HTTP tests, and CI with JS smoke checks.

## 1.0.0

- Initial release of `resilient_ai_stream` built with Dart 3 `sealed class` hierarchies and Clean Architecture.
- Added `AiStreamState`, `AiStreamEvent`, and `AiStreamFailure` sealed hierarchies for exhaustive pattern matching.
- Added `Utf8SseTransformer` for multi-byte UTF-8 safety (prevents Arabic and Emoji corruption across TCP chunk boundaries).
- Added `DefaultPartialJsonHealer` state machine to repair truncated JSON (`input_json_delta` and structured outputs) in real time.
- Added `DefaultHttpStreamTransport` (`dart:io`) and `CustomCallbackTransport` (for `Dio`, `http`, `rhttp`).
- Added `ClaudeSseAdapter`, `CustomRagAdapter`, `CompositeAiAdapter`, and `OpenAiSseAdapter`.
