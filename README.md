# resilient_ai_stream

[![pub package](https://img.shields.io/pub/v/resilient_ai_stream.svg)](https://pub.dev/packages/resilient_ai_stream)
[![Dart validation](https://github.com/Abdalaziz-Tello/resilient_ai_stream/actions/workflows/dart.yml/badge.svg)](https://github.com/Abdalaziz-Tello/resilient_ai_stream/actions/workflows/dart.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A dependency-free Dart engine that turns Claude, OpenAI (Chat Completions and
Responses) and custom RAG SSE streams into sealed UI states, with text, thinking,
assembled tool calls, citations, and token usage. Runs on native Dart/Flutter
and in the browser.

## Features

- **One stream of UI states.** `connecting → thinking → streaming → completed`
  (or `failed`), as a Dart 3 sealed class you can `switch` on exhaustively.
- **Text that never breaks.** UTF-8 is decoded across network chunk
  boundaries, so Arabic, CJK and emoji are never corrupted, even when the
  connection is cut in the middle of a character.
- **Partial answers are kept.** A failure always carries everything that was
  received before it.
- **Tool calls assembled for you**, with a live preview of the unfinished JSON
  arguments.
- **Explicit recovery.** Nothing is resent unless you opt in to `restart` or
  `resume`.
- **Zero dependencies.** Only the Dart SDK; works with `dart:io` and `fetch`.

## Install

```sh
dart pub add resilient_ai_stream
# or, in a Flutter app
flutter pub add resilient_ai_stream
```

## Quick start

Streaming straight from the Claude API (server, CLI or a prototype):

```dart
import 'dart:io';
import 'package:resilient_ai_stream/resilient_ai_stream.dart';

Future<void> main() async {
  final client = ResilientAiStreamClient(adapter: const ClaudeSseAdapter());
  try {
    await for (final state in client.stream(AiStreamRequest(
      url: Uri.parse('https://api.anthropic.com/v1/messages'),
      headers: {
        'x-api-key': Platform.environment['ANTHROPIC_API_KEY']!,
        'anthropic-version': '2023-06-01',
      },
      body: {
        'model': 'claude-haiku-5-5',
        'max_tokens': 300,
        'stream': true,
        'messages': [
          {'role': 'user', 'content': 'ما هي فوائد القراءة؟'}
        ],
      },
    ))) {
      switch (state) {
        case AiStreamStreaming(:final snapshot):
          print(snapshot.text); // the answer so far
        case AiStreamCompleted(:final snapshot):
          print('Done: ${snapshot.usage?.totalTokens} tokens');
        case AiStreamFailed(:final failure, :final snapshot):
          print('${failure.message}; partial answer: ${snapshot.text}');
        default:
          break; // connecting, thinking, tool call, reconnecting
      }
    }
  } finally {
    await client.dispose();
  }
}
```

A runnable version is in
[example/claude_live_example.dart](example/claude_live_example.dart).

> **Never ship a provider API key inside a mobile or web app.** In production,
> point the client at your own backend and send a session token instead:

```dart
final client = ResilientAiStreamClient.claudeWithCustomRag();
final states = client.stream(AiStreamRequest(
  url: Uri.parse('https://your-backend.example/chat/stream'),
  headers: {'Authorization': 'Bearer YOUR_SESSION_TOKEN'},
  body: {'query': 'ما هي سياسة الإجازات؟', 'stream': true},
));
```

### In a Flutter widget

```dart
StreamBuilder<AiStreamState>(
  stream: _states, // create once in initState, not in build
  builder: (context, snap) => switch (snap.data) {
    null || AiStreamConnecting() => const CircularProgressIndicator(),
    AiStreamFailed(:final failure, :final snapshot) =>
      Text('${snapshot.text}\n\n⚠️ ${failure.message}'),
    final state => Text(state.snapshot.text),
  },
);
```

## States

Every value emitted by `client.stream()` is one of these, and every one carries
a `snapshot` with everything received so far.

| State | Meaning |
| --- | --- |
| `AiStreamConnecting` | The request is being opened |
| `AiStreamThinking` | The message started, or reasoning text is arriving |
| `AiStreamSearchingKnowledgeBase` | A tool call or RAG lookup is in progress (`statusMessage`) |
| `AiStreamStreaming` | Answer text or citations are arriving |
| `AiStreamReconnecting` | The stream failed and a retry is scheduled (only with recovery enabled) |
| `AiStreamCompleted` | Terminal: the provider signalled a clean end |
| `AiStreamFailed` | Terminal: carries a typed `failure` and the partial `snapshot` |

`AiStreamSnapshot` fields: `text`, `thinkingText`, `toolBlocks`, `citations`,
`usage`, `finishReason`, `messageId`, `lastEventId`, `healedStructuredJson`.

Failures are typed too: `NetworkStreamFailure`, `HttpStatusStreamFailure`
(`statusCode`, `responseBody`), `ProtocolParseStreamFailure` and
`MaxRetriesExceededFailure`.

## How the package is organised

```text
bytes ──► transport ──► Utf8SseTransformer ──► adapter ──► client ──► AiStreamState
          (HTTP)        (bytes → SSE frames)   (frames →   (reducer,
                                                events)     retries, coalescing)
```

| Layer | Folder | What lives there | You touch it when |
| --- | --- | --- | --- |
| Application | `lib/src/application` | `ResilientAiStreamClient`, the only class most apps need | Always |
| Domain | `lib/src/domain` | States, events, failures, snapshot, request, and the contracts (`AiStreamTransport`, `AiStreamAdapter`, `RetryPolicy`, `JsonHealer`) | Reading states; writing your own adapter or transport |
| Infrastructure | `lib/src/infrastructure` | Protocol adapters (Claude, OpenAI Chat, OpenAI Responses, custom RAG, composite) and transports (`dart:io`, `fetch`, callback) | Choosing a provider or plugging in Dio / `http` |
| Core | `lib/src/core` | SSE parser, partial-JSON healers, update coalescer, backoff policy | Rarely; reusable on their own |

Everything is exported from one import:
`package:resilient_ai_stream/resilient_ai_stream.dart`.

### Other providers

```dart
// OpenAI Chat Completions (also DeepSeek, Groq, vLLM, Azure OpenAI...)
ResilientAiStreamClient(adapter: const OpenAiSseAdapter());

// OpenAI Responses API
ResilientAiStreamClient(adapter: const OpenAiResponsesSseAdapter());

// Don't know which protocol the backend speaks? Route all built-ins.
ResilientAiStreamClient(adapter: CompositeAiAdapter.universal());
```

Token usage and the stop reason are on every snapshot:
`snapshot.usage?.totalTokens`, `snapshot.finishReason`. Tool calls appear in
`snapshot.toolBlocks` with `rawPartialJson`, a live `healedArguments` preview,
and `isComplete`. The engine never executes tools; validate the completed raw
JSON yourself first.

Run `dart run example/resilient_ai_stream_example.dart` for an offline example
that demonstrates an interrupted stream, replay deduplication, and citations.
No provider key or network connection is required for that example.

## Recovery is a backend contract

The default is `StreamRecovery.none`: failures are surfaced with the partial
snapshot, without automatically resending a potentially billable POST request.
A retry policy only takes effect when recovery is explicitly enabled.

| Policy | Behavior | Backend requirement |
| --- | --- | --- |
| `none` (default) | Preserve partial state and report failure | None |
| `restart` | Retry the request and clear all accumulated state before the next attempt | Resending is allowed; may create a new generation and incur another charge |
| `resume` | Keep accumulated state, send `Last-Event-ID`, skip already applied SSE frame IDs | Replay the **same generation**, use a unique explicit nonempty `id:` on every application event, and honor the cursor |

```dart
final client = ResilientAiStreamClient.claudeWithCustomRag(
  recovery: StreamRecovery.resume, // Only with a backend implementing this contract.
  retryPolicy: const ExponentialBackoffPolicy(maxRetries: 3),
);
```

Resume mode rejects application frames without an explicit unique ID rather
than treating an inherited SSE ID as a new frame's identity. Deduplication happens
at the frame level so a frame containing text and citations is applied together.
Processed IDs are retained in memory for the lifetime of that stream. This is a
bounded-generation client, not a client for an indefinitely running event feed.
A changed provider message ID while resuming is rejected.

The package no longer invents an `X-Stream-Offset` header. It does not promise
that provider APIs support resumption just because they use SSE. With direct
provider APIs, use the default policy or explicitly choose restart.

## Completion and failure

Claude `message_stop`, OpenAI-compatible `[DONE]`, and custom RAG
`done: true` / `is_final: true` signal completion. An EOF without a completion
event is an interrupted stream, not a successful answer.

For a custom protocol that intentionally uses EOF as its completion signal,
set `allowCompletionOnEof: true`. That mode cannot distinguish intended EOF from
an interrupted response.

A connection that closes in the middle of a multi-byte character is treated as
an interruption (`NetworkStreamFailure`), so recovery policies can retry it.

Claude in-band `error` events become `ProtocolParseStreamFailure`, preserving
the provider error type/message and partial answer. They are not automatically
retried by the default policy. Unknown event types are ignored for compatibility.
HTTP failures retain their status and body. Malformed UTF-8 is a protocol failure.

## Cancellation and timeouts

- Opening deadline: `openTimeout`, default 30 seconds.
- Byte inactivity deadline: `idleTimeout`, default 60 seconds. Heartbeat bytes reset it.
- Cancel the returned stream subscription to abort that request and stop retries.
- `dispose()` cancels active streams and closes the transport.
- `stream()` is lazy: it opens a request only when listened to.

The native transport cancels the HTTP response subscription as well as aborting
the request. A custom transport **must** wire `request.cancelled` to its own
per-request abort API. The engine cannot forcibly interrupt an arbitrary callback
that ignores cancellation. Late streams returned after cancellation are cancelled.

### Dio integration

```dart
final client = ResilientAiStreamClient(
  transport: CustomCallbackTransport(
    onOpenStream: (request) async {
      final cancelToken = CancelToken();
      request.cancelled?.then((_) => cancelToken.cancel('Stream finished or cancelled'));
      final response = await dio.requestUri<ResponseBody>(
        request.url,
        data: request.body,
        cancelToken: cancelToken,
        options: Options(
          method: request.method,
          headers: request.headers,
          responseType: ResponseType.stream,
        ),
      );
      return response.data!.stream;
    },
  ),
);
```

Dio types in this snippet come from your application's Dio dependency. Custom
transports are responsible for validating HTTP status and mapping failures to
`HttpStatusStreamFailure` when HTTP-aware retry behavior is desired.

## Protocol and platform support

| Adapter / transport | Supported | Limits |
| --- | --- | --- |
| `ClaudeSseAdapter` | Message start/stop, text, thinking, tool JSON deltas, tool completion, citations, usage and stop reason, error events | Does not execute tools or orchestrate subsequent model turns |
| `CustomRagAdapter` | Configurable top-level text, status, citations, completion keys; plain text SSE | Text fields must contain deltas, not cumulative full answers; structured JSON should be well formed |
| `OpenAiSseAdapter` | Chat Completions-style text/reasoning/refusal deltas, parallel `tool_calls` assembly, usage, finish reason, in-band errors, `choiceIndex` selection, `[DONE]` | Surfaces one choice at a time; no legacy `function_call` argument assembly beyond `tool_calls` |
| `OpenAiResponsesSseAdapter` | Responses API text/refusal, reasoning (summary) text, function calls by `output_index`, URL/file citation annotations, usage, completed/incomplete/failed/error | Built-in tool progress events (web search, file search, ...) are ignored |
| `CompositeAiAdapter` | `claudeAndCustomRag()` (default) or `universal()` (Responses, Claude, Chat, custom RAG) | First adapter returning events wins |
| `DefaultHttpStreamTransport` | Native: `dart:io`. Web: `fetch` + `ReadableStream` with abort, back-pressure, status/body failures, optional `includeCredentials` | Requires a `text/event-stream` response; on web the server must allow CORS for your origin and headers (including `Last-Event-ID` for resume) |
| `CustomCallbackTransport` | User-supplied streaming clients (Dio, `http`, ...) | Must wire `request.cancelled` itself |

The default transport is chosen by conditional export: `fetch` in browsers,
`dart:io` natively, and a stub with a typed failure elsewhere. The web transport
is exercised in CI by compiling to JavaScript and streaming from a real local
HTTP server (split multi-byte UTF-8, HTTP errors, wrong content type, abort on
cancel). It has not been tested against live provider endpoints or in every
browser. Browsers cannot hide an API key: call your own backend, not a provider
directly.

The SSE parser follows field whitespace rules, supports LF/CRLF/CR, multiline
data, empty data fields, comments, BOM, and persistent/resettable IDs. Incomplete
SSE events at EOF are discarded. This is a parser and AI stream engine, not a
complete browser EventSource implementation; `retry:` does not override the
configured `RetryPolicy`.

NDJSON is a separate, opt-in extension: set `allowJsonLines: true` when using
an appropriate custom transport. Strict SSE parsing is the default.

## JSON previews and update coalescing

By default `uiCoalesceInterval` (24ms) is a Timer-based notification window.
For Flutter, pass a `frameScheduler` to emit once per real frame instead:

```dart
ResilientAiStreamClient(
  frameScheduler: (cb) {
    WidgetsBinding.instance.scheduleFrameCallback((_) => cb());
    WidgetsBinding.instance.scheduleFrame();
  },
);
```

`Duration.zero` (without a scheduler) emits every state update.

Tool argument previews are produced by `IncrementalJsonHealer`, which advances
its lexical state only over newly arrived text and caches the decoded preview,
so scanning cost is linear in total input rather than re-run per emission. The
single `jsonDecode` per changed preview is native-speed but still proportional to
the accumulated JSON. Healing rules: unfinished strings are closed, dangling or
unfinished object keys are dropped, partial literals/numbers/`\u` escapes are
repaired, trailing commas removed, containers closed. Set
`healStructuredJsonInAnswerText: true` to preview a JSON object in answer text.
Supplying your own `jsonHealer` bypasses the incremental path.

The healer is best-effort: it is **not** a schema validator or proof that a
completed tool call has valid arguments. Validate the original completed JSON
before executing a tool.

## Status text

Engine-generated status text is configurable: `messages: AiStreamMessages.arabic`
(or your own `AiStreamMessages`). Default is English. Backend-supplied status
strings are passed through unchanged.

## Migrating from 1.0.0

- Choose recovery explicitly if automatic retries are wanted.
- Require completion events, or explicitly enable legacy EOF completion.
- Enable `allowJsonLines` explicitly for raw JSON-lines backends.
- Handle `ContentBlockStoppedEvent`, `MessageMetadataEvent` and
  `ToolCallsFinishedEvent` in exhaustive switches over `AiStreamEvent`.
- Tool status text is English by default; use `messages: AiStreamMessages.arabic`.
- `FrameSyncedCoalescer` is now a deprecated alias of `TimerCoalescer`.
- Wire cancellation in custom transports. On web the default transport now works.

See [CHANGELOG.md](CHANGELOG.md) for details.

## Validation

```sh
dart analyze
dart test
dart run example/resilient_ai_stream_example.dart
```

Tests include SSE byte boundaries, interruption/replay, typed provider errors,
restart state reset, timeouts, cancellation, incremental JSON healing (including
prefix and chunking properties), OpenAI Chat/Responses tool calls and usage,
frame-scheduled coalescing, and local HTTP integration. CI also compiles the web
transport to JavaScript and runs it under Node against a local streaming server.

Protocol references: [WHATWG SSE](https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation)
and [Claude streaming](https://platform.claude.com/docs/en/build-with-claude/streaming).

## License

[MIT](LICENSE) © Abdalaziz Tello
