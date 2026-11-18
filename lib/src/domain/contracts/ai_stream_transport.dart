import '../entities/ai_stream_request.dart';

/// Dependency Inversion contract for opening a raw byte stream.
/// Implemented by `DefaultHttpStreamTransport` and `CustomCallbackTransport`.
abstract interface class AiStreamTransport {
  Future<Stream<List<int>>> openByteStream(AiStreamRequest request);
  Future<void> close();
}
