import 'ai_stream_usage.dart';
import 'knowledge_citation.dart';
import 'streaming_tool_block.dart';

/// Immutable accumulator holding all data collected so far during a stream.
/// Carried across every [AiStreamState] subclass so transitions never lose data.
final class AiStreamSnapshot {
  final String? messageId;
  final String text;
  final String thinkingText;
  final List<KnowledgeCitation> citations;
  final Map<int, StreamingToolBlock> toolBlocks;
  final Map<String, dynamic>? healedStructuredJson;
  final String? lastEventId;

  /// Latest token usage reported by the provider, if any.
  final AiStreamUsage? usage;

  /// Provider stop/finish reason (e.g. `end_turn`, `stop`, `tool_calls`).
  final String? finishReason;

  const AiStreamSnapshot({
    this.messageId,
    this.text = '',
    this.thinkingText = '',
    this.citations = const [],
    this.toolBlocks = const {},
    this.healedStructuredJson,
    this.lastEventId,
    this.usage,
    this.finishReason,
  });

  AiStreamSnapshot copyWith({
    String? messageId,
    String? text,
    String? thinkingText,
    List<KnowledgeCitation>? citations,
    Map<int, StreamingToolBlock>? toolBlocks,
    Map<String, dynamic>? healedStructuredJson,
    String? lastEventId,
    AiStreamUsage? usage,
    String? finishReason,
  }) {
    return AiStreamSnapshot(
      messageId: messageId ?? this.messageId,
      text: text ?? this.text,
      thinkingText: thinkingText ?? this.thinkingText,
      citations: citations ?? this.citations,
      toolBlocks: toolBlocks ?? this.toolBlocks,
      healedStructuredJson: healedStructuredJson ?? this.healedStructuredJson,
      lastEventId: lastEventId ?? this.lastEventId,
      usage: usage ?? this.usage,
      finishReason: finishReason ?? this.finishReason,
    );
  }
}
