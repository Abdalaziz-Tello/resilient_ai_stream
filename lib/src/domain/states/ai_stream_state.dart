import '../entities/ai_stream_snapshot.dart';
import '../failures/ai_stream_failure.dart';

/// Dart 3 `sealed class` representing the state of an AI + Knowledge Base stream.
/// Allows exhaustive `switch (state)` pattern matching in UI, Bloc, or Riverpod.
sealed class AiStreamState {
  final AiStreamSnapshot snapshot;
  const AiStreamState({required this.snapshot});

  bool get isTerminal => this is AiStreamCompleted || this is AiStreamFailed;
}

/// Initial state while establishing the HTTP/SSE connection.
final class AiStreamConnecting extends AiStreamState {
  const AiStreamConnecting({
    super.snapshot = const AiStreamSnapshot(),
  });
}

/// Active state when searching the Knowledge Base or invoking a RAG tool.
final class AiStreamSearchingKnowledgeBase extends AiStreamState {
  final String statusMessage;

  const AiStreamSearchingKnowledgeBase({
    required this.statusMessage,
    required super.snapshot,
  });
}

/// Active state when the model is streaming internal reasoning (`thinking_delta`).
final class AiStreamThinking extends AiStreamState {
  const AiStreamThinking({
    required super.snapshot,
  });
}

/// Active state when answer tokens and citations are streaming to the user.
final class AiStreamStreaming extends AiStreamState {
  const AiStreamStreaming({
    required super.snapshot,
  });
}

/// Resilient recovery state when the network drops mid-stream and is reconnecting.
final class AiStreamReconnecting extends AiStreamState {
  final int attempt;
  final int maxRetries;
  final Duration nextRetryDelay;
  final AiStreamFailure lastFailure;

  const AiStreamReconnecting({
    required this.attempt,
    required this.maxRetries,
    required this.nextRetryDelay,
    required this.lastFailure,
    required super.snapshot,
  });
}

/// Terminal state when the response finishes cleanly.
final class AiStreamCompleted extends AiStreamState {
  const AiStreamCompleted({
    required super.snapshot,
  });
}

/// Terminal state when an unrecoverable error occurs or retries are exhausted.
/// Preserves [snapshot] so partial answers already read by the user are never lost!
final class AiStreamFailed extends AiStreamState {
  final AiStreamFailure failure;

  const AiStreamFailed({
    required this.failure,
    required super.snapshot,
  });
}
