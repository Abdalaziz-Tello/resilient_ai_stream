import '../../core/parser/raw_sse_event.dart';
import '../events/ai_stream_event.dart';

/// Strategy Pattern contract for translating a [RawSseEvent] into zero or more
/// strongly-typed [AiStreamEvent] domain events.
abstract interface class AiStreamAdapter {
  List<AiStreamEvent> parseFrame(RawSseEvent frame);
}
