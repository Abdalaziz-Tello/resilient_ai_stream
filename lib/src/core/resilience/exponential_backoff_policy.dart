import '../../domain/contracts/retry_policy.dart';
import '../../domain/failures/ai_stream_failure.dart';

/// Default [RetryPolicy] implementing exponential backoff for transient errors.
final class ExponentialBackoffPolicy implements RetryPolicy {
  @override
  final int maxRetries;
  final Duration initialDelay;

  const ExponentialBackoffPolicy({
    this.maxRetries = 3,
    this.initialDelay = const Duration(milliseconds: 800),
  });

  @override
  bool shouldRetry(AiStreamFailure failure, int attempt) {
    if (attempt > maxRetries) return false;
    return switch (failure) {
      NetworkStreamFailure() => true,
      HttpStatusStreamFailure(:final statusCode) =>
        statusCode == 408 || statusCode == 429 || statusCode >= 500,
      ProtocolParseStreamFailure() => false,
      MaxRetriesExceededFailure() => false,
    };
  }

  @override
  Duration delayForAttempt(int attempt) {
    final multiplier = 1 << (attempt - 1).clamp(0, 6);
    return initialDelay * multiplier;
  }
}
