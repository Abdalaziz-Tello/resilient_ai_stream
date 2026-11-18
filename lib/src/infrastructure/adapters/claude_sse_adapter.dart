import 'dart:convert';
import '../../core/parser/raw_sse_event.dart';
import '../../domain/contracts/ai_stream_adapter.dart';
import '../../domain/entities/ai_stream_usage.dart';
import '../../domain/entities/knowledge_citation.dart';
import '../../domain/events/ai_stream_event.dart';
import '../../domain/failures/ai_stream_failure.dart';

/// Strategy Adapter for official Anthropic Claude Messages API SSE streams.
/// Handles `message_start`, `content_block_start`, `content_block_delta`
/// (`text_delta`, `thinking_delta`, `input_json_delta`, `citations_delta`),
/// `content_block_stop`, `message_delta` (usage / stop reason), and
/// `message_stop`.
final class ClaudeSseAdapter implements AiStreamAdapter {
  const ClaudeSseAdapter();

  @override
  List<AiStreamEvent> parseFrame(RawSseEvent frame) {
    final dataStr = frame.data.trim();
    if (dataStr.isEmpty) return const [];
    if (dataStr == '[DONE]') {
      return [MessageCompletedEvent(eventId: frame.id)];
    }

    final Map<String, dynamic> payload;
    try {
      final decoded = jsonDecode(dataStr);
      if (decoded is! Map<String, dynamic>) return const [];
      payload = decoded;
    } catch (_) {
      return const [];
    }

    final type = (payload['type'] ?? frame.event).toString();

    switch (type) {
      case 'error':
        final error = payload['error'];
        throw ProtocolParseStreamFailure(
          message: error is Map
              ? 'Claude ${error['type']}: ${error['message']}'
              : 'Claude stream error: $error',
          cause: payload,
        );
      case 'message_start':
        final msg = payload['message'];
        final msgId = msg is Map ? msg['id']?.toString() : null;
        final startUsage = msg is Map ? msg['usage'] : null;
        return [
          MessageStartedEvent(messageId: msgId, eventId: frame.id),
          if (startUsage is Map<String, dynamic>)
            MessageMetadataEvent(
                usage: AiStreamUsage.fromJson(startUsage), eventId: frame.id),
        ];

      case 'message_delta':
        final usage = payload['usage'];
        final delta = payload['delta'];
        final stopReason =
            delta is Map ? delta['stop_reason']?.toString() : null;
        if (usage is! Map<String, dynamic> && stopReason == null) {
          return const [];
        }
        return [
          MessageMetadataEvent(
            usage: usage is Map<String, dynamic>
                ? AiStreamUsage.fromJson(usage)
                : null,
            finishReason: stopReason,
            eventId: frame.id,
          ),
        ];

      case 'content_block_start':
        final index = (payload['index'] as int?) ?? 0;
        final block = payload['content_block'];
        if (block is Map<String, dynamic>) {
          final blockType = block['type']?.toString();
          if (blockType == 'tool_use') {
            final toolName = block['name']?.toString() ?? 'knowledge_search';
            final toolId = block['id']?.toString() ?? 'tool_$index';
            return [
              ToolCallDeltaEvent(
                blockIndex: index,
                toolId: toolId,
                toolName: toolName,
                eventId: frame.id,
              ),
            ];
          }
        }
        return const [];

      case 'content_block_delta':
        final index = (payload['index'] as int?) ?? 0;
        final delta = payload['delta'];
        if (delta is Map<String, dynamic>) {
          final deltaType = delta['type']?.toString();
          switch (deltaType) {
            case 'text_delta':
              final text = delta['text']?.toString() ?? '';
              if (text.isEmpty) return const [];
              return [TextDeltaEvent(textDelta: text, eventId: frame.id)];

            case 'thinking_delta':
              final thinking = delta['thinking']?.toString() ?? '';
              if (thinking.isEmpty) return const [];
              return [
                ThinkingDeltaEvent(thinkingDelta: thinking, eventId: frame.id),
              ];

            case 'input_json_delta':
              final partialJson = delta['partial_json']?.toString() ?? '';
              return [
                ToolCallDeltaEvent(
                  blockIndex: index,
                  partialJsonDelta: partialJson,
                  eventId: frame.id,
                ),
              ];

            case 'citations_delta':
              final citationMap = delta['citation'];
              if (citationMap is Map<String, dynamic>) {
                return [
                  CitationsReceivedEvent(
                    citations: [KnowledgeCitation.fromJson(citationMap)],
                    eventId: frame.id,
                  ),
                ];
              }
          }
        }
        return const [];

      case 'content_block_stop':
        final index = (payload['index'] as int?) ?? 0;
        return [
          ContentBlockStoppedEvent(blockIndex: index, eventId: frame.id),
        ];

      case 'message_stop':
        return [MessageCompletedEvent(eventId: frame.id)];

      default:
        return const [];
    }
  }
}
