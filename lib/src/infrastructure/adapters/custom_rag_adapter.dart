import 'dart:convert';
import '../../core/parser/raw_sse_event.dart';
import '../../domain/contracts/ai_stream_adapter.dart';
import '../../domain/entities/knowledge_citation.dart';
import '../../domain/events/ai_stream_event.dart';

/// Strategy Adapter for Custom Knowledge Base / RAG Backend JSON envelopes.
/// Supports customizable JSON keys for text chunks, citations, and search status.
final class CustomRagAdapter implements AiStreamAdapter {
  final List<String> textKeys;
  final List<String> citationListKeys;
  final List<String> statusKeys;

  const CustomRagAdapter({
    this.textKeys = const ['delta', 'text', 'answer', 'content', 'chunk'],
    this.citationListKeys = const [
      'sources',
      'citations',
      'documents',
      'references'
    ],
    this.statusKeys = const ['status', 'step', 'phase', 'progress'],
  });

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
      // If the custom server streams plain unquoted text lines in `data:`
      return [TextDeltaEvent(textDelta: frame.data, eventId: frame.id)];
    }

    // Ignore frames that clearly belong to standard Claude SSE events
    final eventType = payload['type']?.toString();
    if (eventType == 'message_start' ||
        eventType == 'content_block_start' ||
        eventType == 'content_block_delta' ||
        eventType == 'content_block_stop' ||
        eventType == 'message_stop') {
      return const [];
    }

    final events = <AiStreamEvent>[];
    final effectiveId = frame.id ?? payload['id']?.toString();

    // 1. Extract RAG Status updates
    for (final key in statusKeys) {
      final val = payload[key];
      if (val is String && val.isNotEmpty) {
        events.add(StatusUpdateEvent(statusMessage: val, eventId: effectiveId));
        break;
      }
    }

    // 2. Extract Knowledge Base Citations / Sources
    for (final key in citationListKeys) {
      final rawList = payload[key];
      if (rawList is List) {
        final parsed = rawList
            .whereType<Map<String, dynamic>>()
            .map(KnowledgeCitation.fromJson)
            .toList();
        if (parsed.isNotEmpty) {
          events.add(CitationsReceivedEvent(
            citations: parsed,
            eventId: effectiveId,
          ));
        }
        break;
      }
    }

    // 3. Extract Answer Text Delta
    for (final key in textKeys) {
      final val = payload[key];
      if (val is String && val.isNotEmpty) {
        events.add(TextDeltaEvent(textDelta: val, eventId: effectiveId));
        break;
      }
    }

    // 4. Check completion flag
    if (payload['done'] == true || payload['is_final'] == true) {
      events.add(MessageCompletedEvent(eventId: effectiveId));
    }

    return events;
  }
}
