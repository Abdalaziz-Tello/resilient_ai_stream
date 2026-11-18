import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';
import 'package:resilient_ai_stream/resilient_ai_stream.dart';
import 'package:test/test.dart';

final request = AiStreamRequest(url: Uri.parse('https://example.com/stream'));
String sse(Map<String, dynamic> payload, {String? id}) =>
    '${id == null ? '' : 'id: $id\n'}data: ${jsonEncode(payload)}\n\n';
String text(String value, {String? id}) => sse({
      'type': 'content_block_delta',
      'index': 0,
      'delta': {'type': 'text_delta', 'text': value},
    }, id: id);
String done({String? id}) => sse({'type': 'message_stop'}, id: id);
Stream<List<int>> bytes(String data) => Stream.value(utf8.encode(data));
ResilientAiStreamClient client(
  Future<Stream<List<int>>> Function(AiStreamRequest) open, {
  StreamRecovery recovery = StreamRecovery.none,
  int retries = 1,
  Duration delay = Duration.zero,
  Duration idle = const Duration(seconds: 1),
  Duration opening = const Duration(seconds: 1),
  bool eof = false,
  bool jsonLines = false,
  Duration coalesce = Duration.zero,
}) =>
    ResilientAiStreamClient(
      transport: CustomCallbackTransport(onOpenStream: open),
      recovery: recovery,
      retryPolicy:
          ExponentialBackoffPolicy(maxRetries: retries, initialDelay: delay),
      idleTimeout: idle,
      openTimeout: opening,
      uiCoalesceInterval: coalesce,
      allowCompletionOnEof: eof,
      allowJsonLines: jsonLines,
    );

void main() {
  test('JSON previews are healed per emitted snapshot instead of every token',
      () async {
    final healer = CountingHealer();
    final c = ResilientAiStreamClient(
      transport: CustomCallbackTransport(
          onOpenStream: (_) async => bytes(text('{') +
              text('"answer":') +
              text('"hello"') +
              text('}') +
              done())),
      jsonHealer: healer,
      healStructuredJsonInAnswerText: true,
      uiCoalesceInterval: const Duration(milliseconds: 24),
    );
    final states = await c.stream(request).toList();
    expect(states.last.snapshot.healedStructuredJson, {'answer': 'hello'});
    expect(healer.calls, 1);
    await c.dispose();
  });

  test(
      'unexpected EOF preserves partial answer and fails without resending POST',
      () async {
    var calls = 0;
    final c = client((_) async {
      calls++;
      return bytes(text('partial'));
    });
    final states = await c.stream(request).toList();
    expect(states.last, isA<AiStreamFailed>());
    expect(states.last.snapshot.text, 'partial');
    expect(calls, 1);
    await c.dispose();
  });

  test('legacy EOF completion is explicit opt-in', () async {
    final c = client((_) async => bytes(text('partial')), eof: true);
    final states = await c.stream(request).toList();
    expect(states.last, isA<AiStreamCompleted>());
    await c.dispose();
  });

  test('resume deduplicates whole frames and sends only Last-Event-ID',
      () async {
    final requests = <AiStreamRequest>[];
    final first = sse({
      'status': 'search',
      'sources': [
        {'id': 'doc', 'title': 'Doc'}
      ],
      'text': 'hello'
    }, id: '1');
    final c = client((r) async {
      requests.add(r);
      return bytes(requests.length == 1
          ? first
          : first + text(' world', id: '2') + done(id: '3'));
    }, recovery: StreamRecovery.resume);
    final states = await c.stream(request).toList();
    expect(states.last, isA<AiStreamCompleted>());
    expect(states.last.snapshot.text, 'hello world');
    expect(states.last.snapshot.citations, hasLength(1));
    expect(requests[1].headers['Last-Event-ID'], '1');
    expect(requests[1].headers.containsKey('X-Stream-Offset'), isFalse);
    await c.dispose();
  });

  test('resume rejects inherited IDs instead of dropping valid deltas',
      () async {
    final c = client(
        (_) async => bytes(text('one', id: '1') + text('two') + done()),
        recovery: StreamRecovery.resume);
    final states = await c.stream(request).toList();
    expect(states.last, isA<AiStreamFailed>());
    expect((states.last as AiStreamFailed).failure,
        isA<ProtocolParseStreamFailure>());
    expect(states.last.snapshot.text, 'one');
    await c.dispose();
  });

  test('resume refuses a different generation', () async {
    var calls = 0;
    final c = client((_) async {
      calls++;
      return bytes(sse({
        'type': 'message_start',
        'message': {'id': 'message_$calls'}
      }, id: '$calls'));
    }, recovery: StreamRecovery.resume);
    final states = await c.stream(request).toList();
    expect(states.last, isA<AiStreamFailed>());
    expect((states.last as AiStreamFailed).failure.message,
        contains('different message'));
    await c.dispose();
  });

  test('restart clears text, citations and tools before new generation',
      () async {
    var calls = 0;
    final c = client((r) async {
      calls++;
      expect(r.headers.containsKey('Last-Event-ID'), isFalse);
      return bytes(calls == 1
          ? sse({
              'text': 'old',
              'sources': [
                {'id': 'd'}
              ]
            })
          : text('new') + done());
    }, recovery: StreamRecovery.restart);
    final states = await c.stream(request).toList();
    expect(states.last.snapshot.text, 'new');
    expect(states.last.snapshot.citations, isEmpty);
    expect(states.whereType<AiStreamConnecting>(), hasLength(2));
    await c.dispose();
  });

  test('retry exhaustion is bounded and reports actual retry count', () async {
    var calls = 0;
    final c = client((_) async {
      calls++;
      return bytes(text('partial'));
    }, recovery: StreamRecovery.restart, retries: 2);
    final states = await c.stream(request).toList();
    expect(calls, 3);
    final failure =
        (states.last as AiStreamFailed).failure as MaxRetriesExceededFailure;
    expect(failure.attemptsMade, 2);
    await c.dispose();
  });

  test('protocol error after a retry is not mislabeled as exhausted retries',
      () async {
    var calls = 0;
    final c = client((_) async {
      calls++;
      if (calls == 1) throw const NetworkStreamFailure(message: 'offline');
      return bytes(sse({
        'type': 'error',
        'error': {'type': 'invalid_request_error', 'message': 'bad request'}
      }));
    }, recovery: StreamRecovery.restart, retries: 3);
    final states = await c.stream(request).toList();
    expect(calls, 2);
    expect((states.last as AiStreamFailed).failure,
        isA<ProtocolParseStreamFailure>());
    await c.dispose();
  });

  test('Claude in-band error is failure even if message_stop follows',
      () async {
    final c = client((_) async => bytes(text('partial') +
        sse({
          'type': 'error',
          'error': {'type': 'overloaded_error', 'message': 'busy'}
        }) +
        done()));
    final states = await c.stream(request).toList();
    expect(states.last, isA<AiStreamFailed>());
    expect(states.last.snapshot.text, 'partial');
    expect((states.last as AiStreamFailed).failure.message,
        contains('overloaded_error'));
    await c.dispose();
  });

  test('stopping text and thinking blocks creates no tool calls', () async {
    final c = client((_) async => bytes(sse({
          'type': 'content_block_start',
          'index': 0,
          'content_block': {'type': 'text', 'text': ''}
        }) +
        text('hello') +
        sse({'type': 'content_block_stop', 'index': 0}) +
        sse({
          'type': 'content_block_start',
          'index': 1,
          'content_block': {'type': 'thinking', 'thinking': ''}
        }) +
        sse({'type': 'content_block_stop', 'index': 1}) +
        done()));
    final states = await c.stream(request).toList();
    expect(states.last.snapshot.toolBlocks, isEmpty);
    expect(states.last.snapshot.text, 'hello');
    await c.dispose();
  });

  test('tool stop completes only the actual tool with healed arguments',
      () async {
    final c = client((_) async => bytes(sse({
          'type': 'content_block_start',
          'index': 2,
          'content_block': {
            'type': 'tool_use',
            'id': 'tool_id',
            'name': 'search'
          }
        }) +
        sse({
          'type': 'content_block_delta',
          'index': 2,
          'delta': {'type': 'input_json_delta', 'partial_json': '{"q":"مرحبا"}'}
        }) +
        sse({'type': 'content_block_stop', 'index': 2}) +
        done()));
    final states = await c.stream(request).toList();
    final tool = states.last.snapshot.toolBlocks[2]!;
    expect(tool.isComplete, isTrue);
    expect(tool.id, 'tool_id');
    expect(tool.healedArguments, {'q': 'مرحبا'});
    await c.dispose();
  });

  test('coalescing retains final snapshot and emits a single terminal state',
      () async {
    final c = client((_) async => bytes(text('a') + text('b') + done()),
        coalesce: const Duration(milliseconds: 24));
    final states = await c.stream(request).toList();
    expect(states.last.snapshot.text, 'ab');
    expect(states.where((s) => s.isTerminal), hasLength(1));
    await c.dispose();
  });

  test('concrete Uint8List stream does not fail generic transform casts',
      () async {
    final c = client(
        (_) async => Stream<Uint8List>.value(utf8.encode(text('ok') + done())));
    final states = await c.stream(request).toList();
    expect(states.last, isA<AiStreamCompleted>());
    expect(states.last.snapshot.text, 'ok');
    await c.dispose();
  });

  test('idle timeout aborts request and cancels upstream subscription',
      () async {
    final aborted = Completer<void>();
    final upstreamCancelled = Completer<void>();
    final source =
        StreamController<List<int>>(onCancel: upstreamCancelled.complete);
    final c = client((r) async {
      unawaited(r.cancelled!.then((_) => aborted.complete()));
      return source.stream;
    }, idle: const Duration(milliseconds: 30));
    final states = await c.stream(request).toList();
    expect((states.last as AiStreamFailed).failure.message,
        contains('idle timeout'));
    await aborted.future.timeout(const Duration(seconds: 1));
    await upstreamCancelled.future.timeout(const Duration(seconds: 1));
    await source.close();
    await c.dispose();
  });

  test('cancel while opening signals abort without waiting for transport',
      () async {
    final opened = Completer<void>();
    final aborted = Completer<void>();
    final pending = Completer<Stream<List<int>>>();
    final c = client((r) {
      opened.complete();
      unawaited(r.cancelled!.then((_) => aborted.complete()));
      return pending.future;
    }, opening: const Duration(hours: 1));
    final subscription = c.stream(request).listen((_) {});
    await opened.future;
    await subscription.cancel();
    await aborted.future.timeout(const Duration(seconds: 1));
    final lateCancelled = Completer<void>();
    final lateSource =
        StreamController<List<int>>(onCancel: lateCancelled.complete);
    pending.complete(lateSource.stream);
    await lateCancelled.future.timeout(const Duration(seconds: 1));
    await lateSource.close();
    await c.dispose();
  });

  test('opening timeout fails and signals abort', () async {
    final aborted = Completer<void>();
    final c = client((r) {
      unawaited(r.cancelled!.then((_) => aborted.complete()));
      return Completer<Stream<List<int>>>().future;
    }, opening: const Duration(milliseconds: 30));
    final states = await c.stream(request).toList();
    expect(
        (states.last as AiStreamFailed).failure.message, contains('opening'));
    await aborted.future.timeout(const Duration(seconds: 1));
    await c.dispose();
  });

  test('cancel during backoff prevents another request', () async {
    var calls = 0;
    final reconnecting = Completer<void>();
    final c = client((_) async {
      calls++;
      throw const NetworkStreamFailure(message: 'offline');
    }, recovery: StreamRecovery.restart, delay: const Duration(hours: 1));
    final subscription = c.stream(request).listen((s) {
      if (s is AiStreamReconnecting) reconnecting.complete();
    });
    await reconnecting.future;
    await subscription.cancel();
    await c.dispose();
    expect(calls, 1);
  });

  test('dispose closes active streams and cancels upstream', () async {
    final opened = Completer<void>();
    final source = StreamController<List<int>>();
    final c = client((_) async {
      opened.complete();
      return source.stream;
    });
    final complete = c.stream(request).toList();
    await opened.future;
    await c.dispose();
    await complete.timeout(const Duration(seconds: 1));
    await source.close();
  });

  test('custom plain text preserves boundary spaces', () async {
    final c = client(
        (_) async => bytes('data: hello \n\ndata: world\n\ndata: [DONE]\n\n'));
    final states = await c.stream(request).toList();
    expect(states.last.snapshot.text, 'hello world');
    await c.dispose();
  });

  test('restart retries a stream cut in the middle of an Arabic character',
      () async {
    var attempt = 0;
    final full = utf8.encode('event: content_block_delta\n'
        'data: {"type":"content_block_delta","index":0,'
        '"delta":{"type":"text_delta","text":"مرحبا بك"}}\n\n'
        'event: message_stop\ndata: {"type":"message_stop"}\n\n');
    final cutAt = full.indexWhere((b) => b >= 0xD8 && b <= 0xDB) + 1;
    final client = ResilientAiStreamClient(
      adapter: const ClaudeSseAdapter(),
      transport: CustomCallbackTransport(onOpenStream: (request) async {
        attempt++;
        return Stream<List<int>>.value(
            attempt == 1 ? full.sublist(0, cutAt) : full);
      }),
      recovery: StreamRecovery.restart,
      retryPolicy: const ExponentialBackoffPolicy(initialDelay: Duration.zero),
      uiCoalesceInterval: Duration.zero,
    );
    final states = await client
        .stream(AiStreamRequest(url: Uri.parse('https://offline.example/s')))
        .toList();
    await client.dispose();
    expect(attempt, 2);
    expect(states.whereType<AiStreamReconnecting>().single.lastFailure,
        isA<NetworkStreamFailure>());
    expect(states.last, isA<AiStreamCompleted>());
    expect(states.last.snapshot.text, 'مرحبا بك');
  });
}

final class CountingHealer implements JsonHealer {
  int calls = 0;
  @override
  String healJsonString(String value) =>
      const DefaultPartialJsonHealer().healJsonString(value);
  @override
  Map<String, dynamic>? tryParsePartialObject(String value) {
    calls++;
    return const DefaultPartialJsonHealer().tryParsePartialObject(value);
  }
}
