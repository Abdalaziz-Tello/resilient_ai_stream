import 'dart:async';
import '../core/coalescer/timer_coalescer.dart';
import '../core/healer/incremental_json_healer.dart';
import '../core/healer/partial_json_healer.dart';
import '../core/parser/utf8_sse_transformer.dart';
import '../core/parser/raw_sse_event.dart';
import '../core/resilience/exponential_backoff_policy.dart';
import '../domain/contracts/ai_stream_adapter.dart';
import '../domain/contracts/ai_stream_transport.dart';
import '../domain/contracts/json_healer.dart';
import '../domain/contracts/retry_policy.dart';
import '../domain/entities/ai_stream_messages.dart';
import '../domain/entities/ai_stream_request.dart';
import '../domain/entities/stream_recovery.dart';
import '../domain/entities/ai_stream_snapshot.dart';
import '../domain/entities/streaming_tool_block.dart';
import '../domain/events/ai_stream_event.dart';
import '../domain/failures/ai_stream_failure.dart';
import '../domain/states/ai_stream_state.dart';
import '../infrastructure/adapters/composite_ai_adapter.dart';
import '../infrastructure/transports/default_http_transport.dart';

/// High-level orchestrator adhering to Dependency Inversion (DIP).
/// Coordinates transport, UTF-8 SSE decoding, protocol adaptation, partial JSON
/// healing, automatic reconnection, and timer-based state coalescing.
final class ResilientAiStreamClient {
  final AiStreamTransport _transport;
  final AiStreamAdapter _adapter;
  final RetryPolicy _retryPolicy;
  final JsonHealer _jsonHealer;
  final bool _incrementalHealing;
  final Duration uiCoalesceInterval;
  final bool healStructuredJsonInAnswerText;
  final StreamRecovery recovery;
  final Duration idleTimeout;
  final Duration openTimeout;
  final bool allowCompletionOnEof;
  final bool allowJsonLines;

  /// Strings the engine itself shows (e.g. the tool-call status line).
  final AiStreamMessages messages;

  /// Optional frame hook (e.g. Flutter's frame callback). When set, updates are
  /// emitted once per frame and [uiCoalesceInterval] is ignored.
  final FrameScheduler? frameScheduler;
  final Set<void Function()> _activeCancels = {};
  bool _disposed = false;

  ResilientAiStreamClient({
    AiStreamTransport? transport,
    AiStreamAdapter? adapter,
    RetryPolicy? retryPolicy,
    JsonHealer? jsonHealer,
    this.uiCoalesceInterval = const Duration(milliseconds: 24),
    this.healStructuredJsonInAnswerText = false,
    this.recovery = StreamRecovery.none,
    this.idleTimeout = const Duration(seconds: 60),
    this.openTimeout = const Duration(seconds: 30),
    this.allowCompletionOnEof = false,
    this.allowJsonLines = false,
    this.messages = AiStreamMessages.english,
    this.frameScheduler,
  })  : _incrementalHealing = jsonHealer == null,
        _transport = transport ?? DefaultHttpStreamTransport(),
        _adapter = adapter ?? CompositeAiAdapter.claudeAndCustomRag(),
        _retryPolicy = retryPolicy ?? const ExponentialBackoffPolicy(),
        _jsonHealer = jsonHealer ?? const DefaultPartialJsonHealer() {
    if (idleTimeout <= Duration.zero ||
        openTimeout <= Duration.zero ||
        uiCoalesceInterval < Duration.zero) {
      throw ArgumentError(
          'Timeouts must be positive and coalescing non-negative.');
    }
  }

  /// Convenience factory pre-configured for Claude + Custom Knowledge Base APIs.
  factory ResilientAiStreamClient.claudeWithCustomRag({
    AiStreamTransport? transport,
    RetryPolicy? retryPolicy,
    Duration uiCoalesceInterval = const Duration(milliseconds: 24),
    bool healStructuredJsonInAnswerText = false,
    StreamRecovery recovery = StreamRecovery.none,
    Duration idleTimeout = const Duration(seconds: 60),
    Duration openTimeout = const Duration(seconds: 30),
    bool allowCompletionOnEof = false,
    bool allowJsonLines = false,
    AiStreamMessages messages = AiStreamMessages.english,
    FrameScheduler? frameScheduler,
  }) {
    return ResilientAiStreamClient(
      transport: transport,
      adapter: CompositeAiAdapter.claudeAndCustomRag(),
      retryPolicy: retryPolicy,
      uiCoalesceInterval: uiCoalesceInterval,
      healStructuredJsonInAnswerText: healStructuredJsonInAnswerText,
      recovery: recovery,
      idleTimeout: idleTimeout,
      openTimeout: openTimeout,
      allowCompletionOnEof: allowCompletionOnEof,
      allowJsonLines: allowJsonLines,
      messages: messages,
      frameScheduler: frameScheduler,
    );
  }

  /// Opens a resilient stream emitting Dart 3 sealed [AiStreamState] instances.
  Stream<AiStreamState> stream(AiStreamRequest request) {
    final controller = StreamController<AiStreamState>();
    AiStreamSnapshot snapshot = const AiStreamSnapshot();
    AiStreamState currentState = const AiStreamConnecting();
    bool isCancelled = false;
    final cancelled = Completer<void>();
    Completer<void>? activeAbort;
    StreamIterator<RawSseEvent>? iterator;
    final seenIds = <String>{};
    final toolHealers = <int, IncrementalJsonHealer>{};
    final textHealer = IncrementalJsonHealer();
    void cancel() {
      isCancelled = true;
      if (!cancelled.isCompleted) cancelled.complete();
      final abort = activeAbort;
      if (abort != null && !abort.isCompleted) abort.complete();
      unawaited(iterator?.cancel());
      _activeCancels.remove(cancel);
      if (!controller.isClosed) unawaited(controller.close());
    }

    _activeCancels.add(cancel);

    final coalescer = TimerCoalescer<AiStreamState>(
      interval: uiCoalesceInterval,
      frameScheduler: frameScheduler,
      onEmit: (state) {
        if (!isCancelled && !controller.isClosed) {
          // Heal only snapshots that are actually emitted, rather than
          // reparsing the entire accumulated JSON for every incoming token.
          var emitted = state.snapshot;
          final tools = <int, StreamingToolBlock>{};
          for (final entry in emitted.toolBlocks.entries) {
            final raw = entry.value.rawPartialJson;
            final Map<String, dynamic>? parsed;
            if (raw.isEmpty) {
              parsed = null;
            } else if (_incrementalHealing) {
              // Scans only the unseen suffix; the preview is cached per length.
              final healer = toolHealers[entry.key] ??= IncrementalJsonHealer();
              healer.sync(raw);
              parsed = healer.preview;
            } else {
              parsed = _jsonHealer.tryParsePartialObject(raw);
            }
            tools[entry.key] = entry.value.copyWith(healedArguments: parsed);
          }
          emitted = emitted.copyWith(
            toolBlocks: Map.unmodifiable(tools),
            healedStructuredJson:
                healStructuredJsonInAnswerText && emitted.text.isNotEmpty
                    ? (_incrementalHealing
                        ? (textHealer..sync(emitted.text)).preview
                        : _jsonHealer.tryParsePartialObject(emitted.text))
                    : emitted.healedStructuredJson,
          );
          if (identical(snapshot, state.snapshot)) snapshot = emitted;
          controller.add(_withSnapshot(state, emitted));
        }
      },
    );

    /// Pure state transition reducer using exhaustive Dart 3 pattern matching
    /// over the sealed [AiStreamEvent] hierarchy.
    void applyDomainEvent(AiStreamEvent event) {
      final nextEventId = event.eventId ?? snapshot.lastEventId;

      switch (event) {
        case MessageStartedEvent(:final messageId):
          if (recovery == StreamRecovery.resume &&
              snapshot.messageId != null &&
              messageId != null &&
              messageId != snapshot.messageId) {
            throw const ProtocolParseStreamFailure(
                message: 'Backend started a different message while resuming.');
          }
          snapshot = snapshot.copyWith(
            messageId: messageId ?? snapshot.messageId,
            lastEventId: nextEventId,
          );
          currentState = AiStreamThinking(snapshot: snapshot);
          coalescer.schedule(currentState);

        case StatusUpdateEvent(:final statusMessage):
          snapshot = snapshot.copyWith(lastEventId: nextEventId);
          currentState = AiStreamSearchingKnowledgeBase(
            statusMessage: statusMessage,
            snapshot: snapshot,
          );
          coalescer.schedule(currentState, forceImmediate: true);

        case ThinkingDeltaEvent(:final thinkingDelta):
          snapshot = snapshot.copyWith(
            thinkingText: snapshot.thinkingText + thinkingDelta,
            lastEventId: nextEventId,
          );
          currentState = AiStreamThinking(snapshot: snapshot);
          coalescer.schedule(currentState);

        case TextDeltaEvent(:final textDelta):
          final updatedText = snapshot.text + textDelta;
          snapshot = snapshot.copyWith(
            text: updatedText,
            lastEventId: nextEventId,
          );
          currentState = AiStreamStreaming(snapshot: snapshot);
          coalescer.schedule(currentState);

        case ContentBlockStoppedEvent(:final blockIndex):
          // Claude stops text/thinking blocks as well as tools.
          if (snapshot.toolBlocks.containsKey(blockIndex)) {
            applyDomainEvent(ToolCallDeltaEvent(
              blockIndex: blockIndex,
              isCompleted: true,
              eventId: event.eventId,
            ));
          }

        case ToolCallDeltaEvent(
            :final blockIndex,
            :final toolId,
            :final toolName,
            :final partialJsonDelta,
            :final isCompleted,
          ):
          final isNewBlock = !snapshot.toolBlocks.containsKey(blockIndex);
          final existing = snapshot.toolBlocks[blockIndex] ??
              StreamingToolBlock(
                index: blockIndex,
                id: toolId ?? 'tool_$blockIndex',
                name: toolName ?? 'knowledge_tool',
              );

          final combinedJson = existing.rawPartialJson + partialJsonDelta;
          final updatedTools =
              Map<int, StreamingToolBlock>.from(snapshot.toolBlocks);
          updatedTools[blockIndex] = existing.copyWith(
            id: toolId,
            name: toolName,
            rawPartialJson: combinedJson,
            isComplete: isCompleted || existing.isComplete,
          );

          snapshot = snapshot.copyWith(
            toolBlocks: updatedTools,
            lastEventId: nextEventId,
          );
          currentState = AiStreamSearchingKnowledgeBase(
            statusMessage: messages.callingTool(existing.name),
            snapshot: snapshot,
          );
          coalescer.schedule(currentState, forceImmediate: isNewBlock);

        case CitationsReceivedEvent(:final citations):
          snapshot = snapshot.copyWith(
            citations: [...snapshot.citations, ...citations],
            lastEventId: nextEventId,
          );
          currentState = AiStreamStreaming(snapshot: snapshot);
          coalescer.schedule(currentState, forceImmediate: true);

        case MessageMetadataEvent(:final usage, :final finishReason):
          snapshot = snapshot.copyWith(
            usage: usage == null
                ? snapshot.usage
                : (snapshot.usage?.merge(usage) ?? usage),
            finishReason: finishReason,
            lastEventId: nextEventId,
          );
          currentState = _withSnapshot(currentState, snapshot);
          coalescer.schedule(currentState);

        case ToolCallsFinishedEvent():
          final finished = <int, StreamingToolBlock>{
            for (final e in snapshot.toolBlocks.entries)
              e.key: e.value.copyWith(isComplete: true),
          };
          snapshot = snapshot.copyWith(
            toolBlocks: finished,
            lastEventId: nextEventId,
          );
          currentState = _withSnapshot(currentState, snapshot);
          coalescer.schedule(currentState);

        case MessageCompletedEvent():
          snapshot = snapshot.copyWith(lastEventId: nextEventId);
          currentState = AiStreamCompleted(snapshot: snapshot);
          coalescer.schedule(currentState, forceImmediate: true);
      }
    }

    Future<void> executeResilientLoop() async {
      int attempt = 0;
      coalescer.schedule(currentState, forceImmediate: true);
      try {
        if (_disposed) {
          throw const ProtocolParseStreamFailure(
              message: 'Client is disposed.');
        }
        while (!isCancelled) {
          final abort = Completer<void>();
          activeAbort = abort;
          try {
            final headers = <String, String>{
              if (recovery == StreamRecovery.resume &&
                  snapshot.lastEventId != null &&
                  snapshot.lastEventId!.isNotEmpty)
                'Last-Event-ID': snapshot.lastEventId!,
            };
            final effectiveRequest = request
                .copyWithExtraHeaders(headers)
                .withCancellation(abort.future);
            final pending =
                _transport.openByteStream(effectiveRequest).then((bytes) {
              if (abort.isCompleted) {
                unawaited(bytes.listen(null).cancel());
                throw const _StreamCancelled();
              }
              return bytes;
            });
            final deadline = Completer<Stream<List<int>>>();
            final timer = Timer(
                openTimeout,
                () => deadline.completeError(const NetworkStreamFailure(
                    message: 'Timed out opening stream.')));
            final Stream<List<int>> bytes;
            try {
              bytes = await Future.any<Stream<List<int>>>([
                pending,
                deadline.future,
                cancelled.future.then((_) => throw const _StreamCancelled()),
              ]);
            } finally {
              timer.cancel();
            }
            final timedBytes = bytes.timeout(idleTimeout, onTimeout: (sink) {
              sink.addError(const NetworkStreamFailure(
                message: 'Stream idle timeout: no bytes received.',
              ));
              sink.close();
            });
            // bind() avoids runtime generic covariance failures for streams
            // whose concrete element type is Uint8List.
            iterator = StreamIterator(Utf8SseTransformer(
              allowJsonLines: allowJsonLines,
            ).bind(timedBytes));
            while (!isCancelled && await iterator!.moveNext()) {
              if (isCancelled) break;
              final frame = iterator!.current;
              if (recovery == StreamRecovery.resume &&
                  frame.idIsExplicit &&
                  frame.id != null &&
                  seenIds.contains(frame.id)) {
                continue;
              }
              final events = _adapter.parseFrame(frame);
              if (events.isEmpty) continue;
              if (recovery == StreamRecovery.resume &&
                  (!frame.idIsExplicit ||
                      frame.id == null ||
                      frame.id!.isEmpty)) {
                throw const ProtocolParseStreamFailure(
                    message:
                        'Resume requires a unique explicit SSE id on every application event.');
              }
              for (final event in events) {
                applyDomainEvent(event);
                if (currentState is AiStreamCompleted) return;
              }
              if (frame.idIsExplicit && frame.id != null) {
                snapshot = snapshot.copyWith(lastEventId: frame.id);
                if (recovery == StreamRecovery.resume) seenIds.add(frame.id!);
              }
            }
            if (isCancelled) return;
            if (allowCompletionOnEof) {
              currentState = AiStreamCompleted(snapshot: snapshot);
              coalescer.schedule(currentState, forceImmediate: true);
              return;
            }
            throw const NetworkStreamFailure(
              message: 'Stream ended before a completion event was received.',
            );
          } catch (err, st) {
            if (isCancelled || err is _StreamCancelled) return;
            final failure = err is AiStreamFailure
                ? err
                : err is FormatException
                    ? ProtocolParseStreamFailure(
                        message: err.toString(), cause: err, stackTrace: st)
                    : NetworkStreamFailure(
                        message: err.toString(), cause: err, stackTrace: st);
            attempt++;
            final retryAllowed = recovery != StreamRecovery.none &&
                _retryPolicy.shouldRetry(failure, attempt);
            if (!retryAllowed) {
              final exhausted = recovery != StreamRecovery.none &&
                  attempt > _retryPolicy.maxRetries &&
                  _retryPolicy.shouldRetry(failure, 1);
              currentState = AiStreamFailed(
                failure: exhausted
                    ? MaxRetriesExceededFailure(
                        attemptsMade: attempt - 1,
                        lastFailure: failure,
                        message:
                            'Stream failed after ${attempt - 1} retries: ${failure.message}',
                        cause: failure.cause,
                      )
                    : failure,
                snapshot: snapshot,
              );
              coalescer.schedule(currentState, forceImmediate: true);
              return;
            }
            // Abort/cancel the old request before waiting or reconnecting.
            if (!abort.isCompleted) abort.complete();
            await iterator?.cancel();
            iterator = null;
            final delay = _retryPolicy.delayForAttempt(attempt);
            currentState = AiStreamReconnecting(
              attempt: attempt,
              maxRetries: _retryPolicy.maxRetries,
              nextRetryDelay: delay,
              lastFailure: failure,
              snapshot: snapshot,
            );
            coalescer.schedule(currentState, forceImmediate: true);
            final delayDone = Completer<void>();
            final delayTimer = Timer(delay, delayDone.complete);
            try {
              await Future.any<void>([delayDone.future, cancelled.future]);
            } finally {
              delayTimer.cancel();
            }
            if (isCancelled) return;
            if (recovery == StreamRecovery.restart) {
              snapshot = const AiStreamSnapshot();
              seenIds.clear();
              toolHealers.clear();
              textHealer.reset();
              currentState = const AiStreamConnecting();
              coalescer.schedule(currentState, forceImmediate: true);
            }
          } finally {
            if (!abort.isCompleted) abort.complete();
            await iterator?.cancel();
            iterator = null;
          }
        }
      } on AiStreamFailure catch (failure) {
        coalescer.schedule(AiStreamFailed(failure: failure, snapshot: snapshot),
            forceImmediate: true);
      } finally {
        _activeCancels.remove(cancel);
        coalescer.dispose();
        if (!isCancelled) await controller.close();
      }
    }

    controller.onListen = () => unawaited(executeResilientLoop());
    controller.onCancel = () {
      cancel();
      coalescer.dispose();
    };
    return controller.stream;
  }

  Future<void> dispose() async {
    _disposed = true;
    for (final cancel in _activeCancels.toList()) {
      cancel();
    }
    await _transport.close();
  }
}

final class _StreamCancelled implements Exception {
  const _StreamCancelled();
}

AiStreamState _withSnapshot(AiStreamState state, AiStreamSnapshot snapshot) =>
    switch (state) {
      AiStreamConnecting() => AiStreamConnecting(snapshot: snapshot),
      AiStreamSearchingKnowledgeBase(:final statusMessage) =>
        AiStreamSearchingKnowledgeBase(
            statusMessage: statusMessage, snapshot: snapshot),
      AiStreamThinking() => AiStreamThinking(snapshot: snapshot),
      AiStreamStreaming() => AiStreamStreaming(snapshot: snapshot),
      AiStreamReconnecting(
        :final attempt,
        :final maxRetries,
        :final nextRetryDelay,
        :final lastFailure
      ) =>
        AiStreamReconnecting(
            attempt: attempt,
            maxRetries: maxRetries,
            nextRetryDelay: nextRetryDelay,
            lastFailure: lastFailure,
            snapshot: snapshot),
      AiStreamCompleted() => AiStreamCompleted(snapshot: snapshot),
      AiStreamFailed(:final failure) =>
        AiStreamFailed(failure: failure, snapshot: snapshot),
    };
