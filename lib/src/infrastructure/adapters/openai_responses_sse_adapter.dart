import 'dart:convert';
import '../../core/parser/raw_sse_event.dart';
import '../../domain/contracts/ai_stream_adapter.dart';
import '../../domain/entities/ai_stream_usage.dart';
import '../../domain/entities/knowledge_citation.dart';
import '../../domain/events/ai_stream_event.dart';
import '../../domain/failures/ai_stream_failure.dart';

/// Strategy Adapter for the OpenAI Responses API streaming events.
///
/// Handles `response.created`, output text / refusal deltas, reasoning text and
/// summary deltas, function-call items and argument deltas, URL/file citation
/// annotations, token usage, and the terminal `response.completed`,
/// `response.incomplete`, `response.failed` and `error` events. Tool calls are
/// keyed by `output_index`. Unknown event types are ignored.
final class OpenAiResponsesSseAdapter implements AiStreamAdapter {
  const OpenAiResponsesSseAdapter();

  @override
  List<AiStreamEvent> parseFrame(RawSseEvent frame) {
    final dataStr = frame.data.trim();
    if (dataStr.isEmpty || dataStr == '[DONE]') {
      return dataStr.isEmpty
          ? const []
          : [MessageCompletedEvent(eventId: frame.id)];
    }

    final Map<String, dynamic> payload;
    try {
      final decoded = jsonDecode(dataStr);
      if (decoded is! Map<String, dynamic>) return const [];
      payload = decoded;
    } catch (_) {
      return const [];
    }

    final id = frame.id;
    final type = (payload['type'] ?? frame.event).toString();
    final outputIndex =
        payload['output_index'] is int ? payload['output_index'] as int : 0;

    switch (type) {
      case 'response.created':
        final response = payload['response'];
        return [
          MessageStartedEvent(
              messageId: response is Map ? response['id']?.toString() : null,
              eventId: id),
        ];

      case 'response.output_text.delta':
      case 'response.refusal.delta':
        final delta = payload['delta']?.toString() ?? '';
        return delta.isEmpty
            ? const []
            : [TextDeltaEvent(textDelta: delta, eventId: id)];

      case 'response.reasoning_summary_text.delta':
      case 'response.reasoning_text.delta':
        final delta = payload['delta']?.toString() ?? '';
        return delta.isEmpty
            ? const []
            : [ThinkingDeltaEvent(thinkingDelta: delta, eventId: id)];

      case 'response.output_item.added':
        final item = payload['item'];
        if (item is Map && item['type'] == 'function_call') {
          return [
            ToolCallDeltaEvent(
              blockIndex: outputIndex,
              toolId: (item['call_id'] ?? item['id'])?.toString(),
              toolName: item['name']?.toString(),
              eventId: id,
            ),
          ];
        }
        return const [];

      case 'response.function_call_arguments.delta':
        return [
          ToolCallDeltaEvent(
            blockIndex: outputIndex,
            partialJsonDelta: payload['delta']?.toString() ?? '',
            eventId: id,
          ),
        ];

      case 'response.function_call_arguments.done':
        return [ContentBlockStoppedEvent(blockIndex: outputIndex, eventId: id)];

      case 'response.output_text.annotation.added':
        final annotation = payload['annotation'];
        if (annotation is Map<String, dynamic>) {
          final kind = annotation['type'];
          if (kind == 'url_citation' ||
              kind == 'file_citation' ||
              kind == 'container_file_citation') {
            final url = annotation['url']?.toString();
            return [
              CitationsReceivedEvent(
                citations: [
                  KnowledgeCitation(
                    id: (annotation['file_id'] ?? url ?? '').toString(),
                    title: (annotation['title'] ??
                            annotation['filename'] ??
                            'Source')
                        .toString(),
                    urlOrPath: url,
                    metadata: annotation,
                  ),
                ],
                eventId: id,
              ),
            ];
          }
        }
        return const [];

      case 'response.completed':
      case 'response.incomplete':
        final response = payload['response'];
        final usage = response is Map ? response['usage'] : null;
        final details = response is Map ? response['incomplete_details'] : null;
        final finish = type == 'response.completed'
            ? 'completed'
            : (details is Map ? details['reason']?.toString() : null) ??
                'incomplete';
        return [
          MessageMetadataEvent(
            usage: usage is Map<String, dynamic>
                ? AiStreamUsage.fromJson(usage)
                : null,
            finishReason: finish,
            eventId: id,
          ),
          MessageCompletedEvent(eventId: id),
        ];

      case 'response.failed':
        final response = payload['response'];
        final error = response is Map ? response['error'] : null;
        throw ProtocolParseStreamFailure(
          message: error is Map
              ? 'OpenAI response failed (${error['code']}): ${error['message']}'
              : 'OpenAI response failed.',
          cause: payload,
        );

      case 'error':
        // Nested `error` objects belong to other providers (e.g. Claude).
        if (payload['error'] is Map) return const [];
        throw ProtocolParseStreamFailure(
          message:
              'OpenAI stream error (${payload['code']}): ${payload['message']}',
          cause: payload,
        );

      default:
        return const [];
    }
  }
}
