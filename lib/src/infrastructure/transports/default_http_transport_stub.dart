import '../../domain/contracts/ai_stream_transport.dart';
import '../../domain/entities/ai_stream_request.dart';
import '../../domain/failures/ai_stream_failure.dart';

/// Web applications must provide a streaming, cancellable browser transport.
final class DefaultHttpStreamTransport implements AiStreamTransport {
  final Duration connectionTimeout;
  DefaultHttpStreamTransport({
    Object? client,
    this.connectionTimeout = const Duration(seconds: 20),
  });

  @override
  Future<Stream<List<int>>> openByteStream(AiStreamRequest request) async {
    throw const ProtocolParseStreamFailure(
        message: 'DefaultHttpStreamTransport requires dart:io. On web, supply '
            'CustomCallbackTransport with a browser-compatible streaming HTTP client.');
  }

  @override
  Future<void> close() async {}
}
