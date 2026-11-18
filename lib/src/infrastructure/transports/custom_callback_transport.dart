import '../../domain/contracts/ai_stream_transport.dart';
import '../../domain/entities/ai_stream_request.dart';
import '../../domain/failures/ai_stream_failure.dart';

/// Adapter implementation of [AiStreamTransport] allowing seamless integration
/// with external HTTP clients such as `Dio`, `http`, or `rhttp`.
final class CustomCallbackTransport implements AiStreamTransport {
  final Future<Stream<List<int>>> Function(AiStreamRequest request)
      onOpenStream;
  final Future<void> Function()? onClose;

  const CustomCallbackTransport({
    required this.onOpenStream,
    this.onClose,
  });

  @override
  Future<Stream<List<int>>> openByteStream(AiStreamRequest request) async {
    try {
      return await onOpenStream(request);
    } on AiStreamFailure {
      rethrow;
    } catch (e, st) {
      throw NetworkStreamFailure(
        message: 'External transport failed to open stream: $e',
        cause: e,
        stackTrace: st,
      );
    }
  }

  @override
  Future<void> close() async {
    await onClose?.call();
  }
}
