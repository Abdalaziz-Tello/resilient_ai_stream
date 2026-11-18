import 'dart:async';
import 'dart:convert';
import 'dart:js_interop';
import 'dart:js_interop_unsafe';
import 'dart:typed_data';

import '../../domain/contracts/ai_stream_transport.dart';
import '../../domain/entities/ai_stream_request.dart';
import '../../domain/failures/ai_stream_failure.dart';

@JS('fetch')
external JSPromise<_FetchResponse> _fetch(String url, JSObject init);

@JS('AbortController')
extension type _AbortController._(JSObject _) implements JSObject {
  external _AbortController();
  external JSObject get signal;
  external void abort();
}

extension type _FetchResponse._(JSObject _) implements JSObject {
  external bool get ok;
  external int get status;
  external _FetchHeaders get headers;
  external _ReadableStream? get body;
  external JSPromise<JSString> text();
}

extension type _FetchHeaders._(JSObject _) implements JSObject {
  external String? get(String name);
}

extension type _ReadableStream._(JSObject _) implements JSObject {
  external _Reader getReader();
}

extension type _Reader._(JSObject _) implements JSObject {
  external JSPromise<_ReadResult> read();
  external JSPromise<JSAny?> cancel();
}

extension type _ReadResult._(JSObject _) implements JSObject {
  external bool get done;
  external JSUint8Array? get value;
}

/// Browser implementation of [AiStreamTransport] built on `fetch` and
/// `ReadableStream`, with no package dependencies (only `dart:js_interop`).
///
/// Notes for web apps:
///  * the server must allow your origin via CORS (including the request headers
///    you send, and `Last-Event-ID` when using resume);
///  * `fetch` cannot read the response while it is still being uploaded, so
///    this is a request/response streaming transport, not full duplex.
final class DefaultHttpStreamTransport implements AiStreamTransport {
  final Duration connectionTimeout;

  /// Sends cookies / HTTP auth cross-origin (`credentials: "include"`).
  final bool includeCredentials;

  final Set<_AbortController> _active = {};

  DefaultHttpStreamTransport({
    Object? client,
    this.connectionTimeout = const Duration(seconds: 20),
    this.includeCredentials = false,
  });

  @override
  Future<Stream<List<int>>> openByteStream(AiStreamRequest request) async {
    final abort = _AbortController();
    _active.add(abort);
    var stopped = false;
    void stop() {
      stopped = true;
      _active.remove(abort);
      abort.abort();
    }

    unawaited(request.cancelled?.then((_) => stop()));

    try {
      final headers = JSObject()
        ..['Accept'] = 'text/event-stream'.toJS
        ..['Cache-Control'] = 'no-cache'.toJS;
      request.headers.forEach((key, value) => headers[key] = value.toJS);

      final init = JSObject()
        ..['method'] = request.method.toJS
        ..['headers'] = headers
        ..['signal'] = abort.signal
        ..['cache'] = 'no-store'.toJS;
      if (includeCredentials) init['credentials'] = 'include'.toJS;
      if (request.body != null) {
        headers['Content-Type'] = 'application/json; charset=utf-8'.toJS;
        init['body'] = jsonEncode(request.body).toJS;
      }

      final response = await _fetch(request.url.toString(), init)
          .toDart
          .timeout(connectionTimeout, onTimeout: () {
        stop();
        throw const NetworkStreamFailure(message: 'Timed out connecting.');
      });

      if (!response.ok) {
        String body = '';
        try {
          body = (await response.text().toDart).toDart;
        } catch (_) {}
        stop();
        throw HttpStatusStreamFailure(
          statusCode: response.status,
          responseBody: body,
          message: 'HTTP ${response.status}: $body',
        );
      }
      final contentType = (response.headers.get('content-type') ?? '')
          .split(';')
          .first
          .trim()
          .toLowerCase();
      if (contentType != 'text/event-stream') {
        stop();
        throw const ProtocolParseStreamFailure(
            message: 'Expected a text/event-stream response.');
      }
      final body = response.body;
      if (body == null) {
        stop();
        throw const ProtocolParseStreamFailure(
            message: 'The browser returned no readable response body.');
      }
      if (stopped) {
        throw const NetworkStreamFailure(message: 'Request cancelled.');
      }
      return _pump(body.getReader(), abort, stop, () => stopped);
    } on AiStreamFailure {
      rethrow;
    } catch (e, st) {
      stop();
      throw NetworkStreamFailure(
        message: 'Network error while opening AI stream: $e',
        cause: e,
        stackTrace: st,
      );
    }
  }

  Stream<List<int>> _pump(
    _Reader reader,
    _AbortController abort,
    void Function() stop,
    bool Function() isStopped,
  ) {
    late final StreamController<List<int>> controller;
    Completer<void>? resumed;
    var started = false;

    Future<void> run() async {
      try {
        while (!isStopped()) {
          if (controller.isPaused) {
            await (resumed ??= Completer<void>()).future;
            resumed = null;
            continue;
          }
          final result = await reader.read().toDart;
          if (result.done) break;
          final chunk = result.value;
          if (chunk != null && !controller.isClosed) {
            controller.add(Uint8List.fromList(chunk.toDart));
          }
        }
      } catch (e, st) {
        if (!isStopped() && !controller.isClosed) {
          controller.addError(
              NetworkStreamFailure(
                message: 'Network error while reading AI stream: $e',
                cause: e,
                stackTrace: st,
              ),
              st);
        }
      } finally {
        _active.remove(abort);
        if (!controller.isClosed) await controller.close();
      }
    }

    controller = StreamController<List<int>>(
      onListen: () {
        if (started) return;
        started = true;
        unawaited(run());
      },
      onResume: () {
        final waiting = resumed;
        if (waiting != null && !waiting.isCompleted) waiting.complete();
      },
      onCancel: () async {
        stop();
        try {
          await reader.cancel().toDart;
        } catch (_) {}
        final waiting = resumed;
        if (waiting != null && !waiting.isCompleted) waiting.complete();
      },
    );
    return controller.stream;
  }

  @override
  Future<void> close() async {
    for (final abort in _active.toList()) {
      abort.abort();
    }
    _active.clear();
  }
}
