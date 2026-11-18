import '../../core/parser/raw_sse_event.dart';
import '../../domain/contracts/ai_stream_adapter.dart';
import '../../domain/events/ai_stream_event.dart';
import 'claude_sse_adapter.dart';
import 'custom_rag_adapter.dart';
import 'openai_responses_sse_adapter.dart';
import 'openai_sse_adapter.dart';

/// Composite Design Pattern: Chains multiple [AiStreamAdapter] strategies
/// so a single client can seamlessly parse both native Claude SSE events AND
/// custom Knowledge Base backend JSON wrappers in the same stream!
final class CompositeAiAdapter implements AiStreamAdapter {
  final List<AiStreamAdapter> adapters;

  const CompositeAiAdapter(this.adapters);

  /// Pre-configured composite adapter supporting both Official Anthropic Claude
  /// SSE events and Custom Knowledge Base (RAG) JSON formats.
  factory CompositeAiAdapter.claudeAndCustomRag() {
    return const CompositeAiAdapter([
      ClaudeSseAdapter(),
      CustomRagAdapter(),
    ]);
  }

  /// Every built-in protocol: OpenAI Responses, Claude, OpenAI Chat
  /// Completions, then the permissive custom RAG envelope as the last resort.
  factory CompositeAiAdapter.universal() {
    return const CompositeAiAdapter([
      OpenAiResponsesSseAdapter(),
      ClaudeSseAdapter(),
      OpenAiSseAdapter(),
      CustomRagAdapter(),
    ]);
  }

  @override
  List<AiStreamEvent> parseFrame(RawSseEvent frame) {
    for (final adapter in adapters) {
      final events = adapter.parseFrame(frame);
      if (events.isNotEmpty) {
        return events;
      }
    }
    return const [];
  }
}
