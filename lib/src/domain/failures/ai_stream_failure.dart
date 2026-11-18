/// Sealed hierarchy of typed domain failures for exhaustive error handling.
sealed class AiStreamFailure {
  final String message;
  final Object? cause;
  final StackTrace? stackTrace;

  const AiStreamFailure({
    required this.message,
    this.cause,
    this.stackTrace,
  });
}

/// Socket, DNS, or connection drop failure.
final class NetworkStreamFailure extends AiStreamFailure {
  const NetworkStreamFailure({
    required super.message,
    super.cause,
    super.stackTrace,
  });
}

/// Non-2xx HTTP status code returned by the server.
final class HttpStatusStreamFailure extends AiStreamFailure {
  final int statusCode;
  final String? responseBody;

  const HttpStatusStreamFailure({
    required this.statusCode,
    this.responseBody,
    required super.message,
    super.cause,
    super.stackTrace,
  });
}

/// Unrecoverable protocol or parsing failure.
final class ProtocolParseStreamFailure extends AiStreamFailure {
  const ProtocolParseStreamFailure({
    required super.message,
    super.cause,
    super.stackTrace,
  });
}

/// All automatic reconnection attempts were exhausted.
final class MaxRetriesExceededFailure extends AiStreamFailure {
  final int attemptsMade;
  final AiStreamFailure lastFailure;

  const MaxRetriesExceededFailure({
    required this.attemptsMade,
    required this.lastFailure,
    required super.message,
    super.cause,
    super.stackTrace,
  });
}
