import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:resilient_ai_stream/resilient_ai_stream.dart';
import 'package:test/test.dart';

void main() {
  late HttpServer server;
  late Uri url;
  setUp(() async {
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    url = Uri.parse('http://127.0.0.1:${server.port}/stream');
  });
  tearDown(() => server.close(force: true));

  test('native HTTP streams Arabic and preserves request method/body/headers',
      () async {
    final received = Completer<void>();
    final serving = server.first.then((r) async {
      expect(r.method, 'POST');
      expect(r.headers.value('authorization'), 'Bearer test');
      expect(jsonDecode(await utf8.decoder.bind(r).join()), {'query': 'مرحبا'});
      r.response.headers.contentType =
          ContentType('text', 'event-stream', charset: 'utf-8');
      final data = utf8.encode('data: {"text":"مرحبا 👋"}\n\ndata: [DONE]\n\n');
      for (final byte in data) {
        r.response.add([byte]);
      }
      await r.response.close();
      received.complete();
    });
    final c = ResilientAiStreamClient();
    final states = await c
        .stream(AiStreamRequest(
            url: url,
            headers: {'Authorization': 'Bearer test'},
            body: {'query': 'مرحبا'}))
        .toList();
    expect(states.last, isA<AiStreamCompleted>());
    expect(states.last.snapshot.text, 'مرحبا 👋');
    await received.future;
    await serving;
    await c.dispose();
  });

  test('native HTTP rejects a successful non-SSE response', () async {
    final serving = server.first.then((r) async {
      r.response.headers.contentType = ContentType.json;
      r.response.write('{"text":"this is not SSE"}');
      await r.response.close();
    });
    final c = ResilientAiStreamClient();
    final states = await c.stream(AiStreamRequest(url: url)).toList();
    expect((states.last as AiStreamFailed).failure,
        isA<ProtocolParseStreamFailure>());
    await serving;
    await c.dispose();
  });

  test('native HTTP status is preserved as a typed failure', () async {
    final serving = server.first.then((r) async {
      r.response.statusCode = 401;
      r.response.write('unauthorized');
      await r.response.close();
    });
    final c = ResilientAiStreamClient();
    final states = await c.stream(AiStreamRequest(url: url)).toList();
    final failure =
        (states.last as AiStreamFailed).failure as HttpStatusStreamFailure;
    expect(failure.statusCode, 401);
    expect(failure.responseBody, 'unauthorized');
    await serving;
    await c.dispose();
  });

  test('cancelling native HTTP stream closes an unfinished response', () async {
    final disconnected = Completer<void>();
    final serving = server.first.then((r) async {
      r.response.headers.contentType = ContentType('text', 'event-stream');
      // An intentionally unfinished fixed-length HTTP body lets the server
      // observe the client's FIN directly through the detached socket.
      r.response.contentLength = 1024 * 1024;
      final socket = await r.response.detachSocket();
      addTearDown(socket.destroy);
      socket.listen((_) {}, onDone: disconnected.complete, onError: (Object _) {
        if (!disconnected.isCompleted) disconnected.complete();
      });
      socket.write('data: {"text":"partial"}\n\n');
      await socket.flush();
    });
    final c = ResilientAiStreamClient(uiCoalesceInterval: Duration.zero);
    final partial = Completer<void>();
    final subscription = c.stream(AiStreamRequest(url: url)).listen((s) {
      if (s.snapshot.text == 'partial' && !partial.isCompleted) {
        partial.complete();
      }
    });
    await partial.future.timeout(const Duration(seconds: 2));
    await subscription.cancel();
    await disconnected.future.timeout(const Duration(seconds: 2));
    await serving;
    await c.dispose();
  });
}
