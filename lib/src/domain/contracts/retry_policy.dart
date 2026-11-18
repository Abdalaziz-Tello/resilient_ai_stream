import '../failures/ai_stream_failure.dart';

/// Strategy Pattern contract controlling when and how to retry a dropped stream.
abstract interface class RetryPolicy {
  int get maxRetries;
  bool shouldRetry(AiStreamFailure failure, int attempt);
  Duration delayForAttempt(int attempt);
}
