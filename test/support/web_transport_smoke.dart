import 'dart:async';
import 'dart:js_interop';
import 'dart:js_interop_unsafe';
import 'package:resilient_ai_stream/resilient_ai_stream.dart';

/// Compiled to JS and driven by web_transport_smoke.js, which serves the SSE
/// endpoints below from a real local HTTP server and reads `__smokeResult`.
Future<void> main() async {
  String base() => (globalContext['__SSE_BASE__'] as JSString).toDart;
  void check(bool ok, String what) {
    if (!ok) throw StateError('FAILED: $what');
  }

  try {
    // 1. Real fetch streaming, Arabic split across chunks, headers + body.
    final client = ResilientAiStreamClient(uiCoalesceInterval: Duration.zero);
    var states = await client
        .stream(AiStreamRequest(
          url: Uri.parse('${base()}/ok'),
          headers: {'X-Test': 'yes'},
          body: {'q': 'مرحبا'},
        ))
        .toList();
    check(states.last is AiStreamCompleted, 'completes: ${states.last}');
    check(states.last.snapshot.text == 'مرحبا بك 👋',
        'utf8 across chunks: ${states.last.snapshot.text}');
    check(states.last.snapshot.citations.length == 1, 'citations');

    // 2. HTTP errors keep status and body.
    states = await client
        .stream(AiStreamRequest(url: Uri.parse('${base()}/status500')))
        .toList();
    final failed = states.last;
    check(failed is AiStreamFailed, 'fails on 500');
    final failure = (failed as AiStreamFailed).failure;
    check(failure is HttpStatusStreamFailure && failure.statusCode == 500,
        'status kept: $failure');
    check(failure.message.contains('boom'), 'body kept');

    // 3. Wrong content type is a protocol failure.
    states = await client
        .stream(AiStreamRequest(url: Uri.parse('${base()}/json')))
        .toList();
    check(
        states.last is AiStreamFailed &&
            (states.last as AiStreamFailed).failure
                is ProtocolParseStreamFailure,
        'content type check');

    // 4. Cancelling aborts the in-flight fetch (server observes the close).
    final sub = client
        .stream(AiStreamRequest(url: Uri.parse('${base()}/slow')))
        .listen((_) {});
    await Future<void>.delayed(const Duration(milliseconds: 300));
    await sub.cancel();
    await Future<void>.delayed(const Duration(milliseconds: 300));

    // 5. Universal adapter over the OpenAI Responses protocol.
    final universal = ResilientAiStreamClient(
        adapter: CompositeAiAdapter.universal(),
        uiCoalesceInterval: Duration.zero);
    states = await universal
        .stream(AiStreamRequest(url: Uri.parse('${base()}/responses')))
        .toList();
    check(states.last is AiStreamCompleted, 'responses completes');
    check(states.last.snapshot.text == 'hi there', 'responses text');
    check(states.last.snapshot.usage?.totalTokens == 7, 'responses usage');

    await client.dispose();
    await universal.dispose();
    globalContext['__smokeResult'] = 'ok'.toJS;
  } catch (e) {
    globalContext['__smokeResult'] = 'error: $e'.toJS;
  }
}
