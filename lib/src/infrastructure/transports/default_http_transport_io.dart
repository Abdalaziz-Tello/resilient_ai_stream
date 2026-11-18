import 'dart:async';
import 'dart:convert';
import 'dart:io';
import '../../domain/contracts/ai_stream_transport.dart';
import '../../domain/entities/ai_stream_request.dart';
import '../../domain/failures/ai_stream_failure.dart';

/// Zero-dependency implementation of [AiStreamTransport] using `dart:io`.
final class DefaultHttpStreamTransport implements AiStreamTransport {
  final HttpClient _client;
  final Duration connectionTimeout;

  DefaultHttpStreamTransport({
    HttpClient? client,
    this.connectionTimeout = const Duration(seconds: 20),
  }) : _client = client ?? HttpClient() {
    _client.connectionTimeout = connectionTimeout;
  }

  @override
  Future<Stream<List<int>>> openByteStream(AiStreamRequest request) async {
    try {
      bool cancelled = false;
      HttpClientRequest? activeRequest;
      unawaited(request.cancelled?.then((_) {
        cancelled = true;
        activeRequest?.abort();
      }));
      final ioReq = await _client.openUrl(request.method, request.url);
      activeRequest = ioReq;
      if (cancelled) {
        ioReq.abort();
        throw const NetworkStreamFailure(message: 'Request cancelled.');
      }
      ioReq.headers.set(HttpHeaders.acceptHeader, 'text/event-stream');
      ioReq.headers.set(HttpHeaders.cacheControlHeader, 'no-cache');

      request.headers.forEach((key, value) {
        ioReq.headers.set(key, value);
      });

      if (request.body != null) {
        ioReq.headers.set(
          HttpHeaders.contentTypeHeader,
          'application/json; charset=utf-8',
        );
        ioReq.add(utf8.encode(jsonEncode(request.body)));
      }

      final response = await ioReq.close();
      if (response.statusCode < 200 || response.statusCode >= 300) {
        final errBody = await utf8.decoder.bind(response).join();
        throw HttpStatusStreamFailure(
          statusCode: response.statusCode,
          responseBody: errBody,
          message: 'HTTP ${response.statusCode}: $errBody',
        );
      }
      if (response.headers.contentType?.mimeType != 'text/event-stream') {
        ioReq.abort();
        throw const ProtocolParseStreamFailure(
          message: 'Expected a text/event-stream response.',
        );
      }
      return _cancellableResponse(response, request.cancelled);
    } on AiStreamFailure {
      rethrow;
    } catch (e, st) {
      throw NetworkStreamFailure(
        message: 'Network error while opening AI stream: $e',
        cause: e,
        stackTrace: st,
      );
    }
  }

  @override
  Future<void> close() async {
    _client.close(force: true);
  }
}

/// Cancels the response subscription as well as the request: abort() alone is
/// insufficient once the response headers have already arrived.
Stream<List<int>> _cancellableResponse(
  HttpClientResponse response,
  Future<void>? cancelled,
) {
  StreamSubscription<List<int>>? subscription;
  late final StreamController<List<int>> controller;
  bool stopped = false;
  Future<void> stop() async {
    stopped = true;
    await subscription?.cancel();
    if (!controller.isClosed) unawaited(controller.close());
  }

  controller = StreamController<List<int>>(
    onListen: () {
      subscription = response.listen(
        controller.add,
        onError: controller.addError,
        onDone: controller.close,
      );
      if (stopped) unawaited(stop());
    },
    onPause: () => subscription?.pause(),
    onResume: () => subscription?.resume(),
    onCancel: stop,
  );
  unawaited(cancelled?.then((_) => stop()));
  return controller.stream;
}
