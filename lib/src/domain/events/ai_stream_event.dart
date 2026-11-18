import '../entities/ai_stream_usage.dart';
import '../entities/knowledge_citation.dart';

/// Sealed hierarchy of domain events parsed by any [AiStreamAdapter].
/// Enables exhaustive Dart 3 `switch` pattern matching inside the reducer.
sealed class AiStreamEvent {
  final String? eventId;
  const AiStreamEvent({this.eventId});
}

/// Emitted when the AI stream initializes a new message (`message_start`).
final class MessageStartedEvent extends AiStreamEvent {
  final String? messageId;
  const MessageStartedEvent({this.messageId, super.eventId});
}

/// Emitted when the backend or model reports a RAG / Knowledge Base status step.
final class StatusUpdateEvent extends AiStreamEvent {
  final String statusMessage;
  const StatusUpdateEvent({required this.statusMessage, super.eventId});
}

/// Emitted when extended thinking / reasoning tokens arrive (`thinking_delta`).
final class ThinkingDeltaEvent extends AiStreamEvent {
  final String thinkingDelta;
  const ThinkingDeltaEvent({required this.thinkingDelta, super.eventId});
}

/// Emitted when answer text tokens arrive (`text_delta` or custom `delta`).
final class TextDeltaEvent extends AiStreamEvent {
  final String textDelta;
  const TextDeltaEvent({required this.textDelta, super.eventId});
}

/// Emitted when a tool call (`tool_use`) starts or streams `input_json_delta`.
final class ToolCallDeltaEvent extends AiStreamEvent {
  final int blockIndex;
  final String? toolId;
  final String? toolName;
  final String partialJsonDelta;
  final bool isCompleted;

  const ToolCallDeltaEvent({
    required this.blockIndex,
    this.toolId,
    this.toolName,
    this.partialJsonDelta = '',
    this.isCompleted = false,
    super.eventId,
  });
}

/// Emitted when one or more Knowledge Base citations arrive (`citations_delta` or `sources`).
final class CitationsReceivedEvent extends AiStreamEvent {
  final List<KnowledgeCitation> citations;
  const CitationsReceivedEvent({required this.citations, super.eventId});
}

/// Emitted when the stream signals normal completion (`message_stop` or `[DONE]`).
final class MessageCompletedEvent extends AiStreamEvent {
  const MessageCompletedEvent({super.eventId});
}

/// A provider content block ended; only an existing tool block is completed.
final class ContentBlockStoppedEvent extends AiStreamEvent {
  final int blockIndex;
  const ContentBlockStoppedEvent({required this.blockIndex, super.eventId});
}

/// Provider metadata that arrives alongside the answer: token usage and/or the
/// finish reason. Does not change the visible stream phase.
final class MessageMetadataEvent extends AiStreamEvent {
  final AiStreamUsage? usage;
  final String? finishReason;
  const MessageMetadataEvent({this.usage, this.finishReason, super.eventId});
}

/// The provider signalled that every pending tool call is fully streamed
/// (e.g. OpenAI `finish_reason: "tool_calls"`) without naming block indexes.
final class ToolCallsFinishedEvent extends AiStreamEvent {
  const ToolCallsFinishedEvent({super.eventId});
}
