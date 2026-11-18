import 'dart:convert';
import '../../core/parser/raw_sse_event.dart';
import '../../domain/contracts/ai_stream_adapter.dart';
import '../../domain/entities/ai_stream_usage.dart';
import '../../domain/events/ai_stream_event.dart';
import '../../domain/failures/ai_stream_failure.dart';

/// Strategy Adapter for OpenAI Chat Completions-style SSE streams (OpenAI,
/// Azure OpenAI, DeepSeek, Groq, vLLM, ...).
///
/// Supports text and reasoning deltas, streamed `tool_calls` (assembled by the
/// client into tool blocks), refusals, token usage (`stream_options.include_usage`),
/// finish reasons, in-band `error` objects, and selecting which choice to read
/// via [choiceIndex]. For the Responses API use [OpenAiResponsesSseAdapter].
final class OpenAiSseAdapter implements AiStreamAdapter {
  /// The `choices[].index` whose deltas are surfaced (others are ignored).
  final int choiceIndex;

  const OpenAiSseAdapter({this.choiceIndex = 0});

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

    final id = frame.id;
    final error = payload['error'];
    if (error != null && payload['choices'] == null) {
      throw ProtocolParseStreamFailure(
        message: error is Map
            ? 'OpenAI ${error['type'] ?? error['code'] ?? 'error'}: ${error['message']}'
            : 'OpenAI stream error: $error',
        cause: payload,
      );
    }

    final events = <AiStreamEvent>[];
    final choices = payload['choices'];
    if (choices is List) {
      for (var position = 0; position < choices.length; position++) {
        final choice = choices[position];
        if (choice is! Map<String, dynamic>) continue;
        final index =
            choice['index'] is int ? choice['index'] as int : position;
        if (index != choiceIndex) continue;
        _readChoice(payload, choice, id, events);
      }
    }

    final usage = payload['usage'];
    if (usage is Map<String, dynamic>) {
      events.add(MessageMetadataEvent(
          usage: AiStreamUsage.fromJson(usage), eventId: id));
    }
    return events;
  }

  void _readChoice(
    Map<String, dynamic> payload,
    Map<String, dynamic> choice,
    String? id,
    List<AiStreamEvent> events,
  ) {
    final delta = choice['delta'];
    if (delta is Map<String, dynamic>) {
      if (delta['role'] != null) {
        events.add(MessageStartedEvent(
            messageId: payload['id']?.toString(), eventId: id));
      }
      final reasoning =
          (delta['reasoning_content'] ?? delta['reasoning'])?.toString() ?? '';
      if (reasoning.isNotEmpty) {
        events.add(ThinkingDeltaEvent(thinkingDelta: reasoning, eventId: id));
      }
      final content = delta['content']?.toString() ?? '';
      if (content.isNotEmpty) {
        events.add(TextDeltaEvent(textDelta: content, eventId: id));
      }
      final refusal = delta['refusal']?.toString() ?? '';
      if (refusal.isNotEmpty) {
        events.add(TextDeltaEvent(textDelta: refusal, eventId: id));
      }
      final toolCalls = delta['tool_calls'];
      if (toolCalls is List) {
        for (var position = 0; position < toolCalls.length; position++) {
          final call = toolCalls[position];
          if (call is! Map<String, dynamic>) continue;
          final function = call['function'];
          events.add(ToolCallDeltaEvent(
            blockIndex: call['index'] is int ? call['index'] as int : position,
            toolId: call['id']?.toString(),
            toolName: function is Map ? function['name']?.toString() : null,
            partialJsonDelta:
                function is Map ? function['arguments']?.toString() ?? '' : '',
            eventId: id,
          ));
        }
      }
    }

    final finish = choice['finish_reason']?.toString();
    if (finish != null && finish.isNotEmpty) {
      if (finish == 'tool_calls' || finish == 'function_call') {
        events.add(ToolCallsFinishedEvent(eventId: id));
      }
      events.add(MessageMetadataEvent(finishReason: finish, eventId: id));
    }
  }
}
