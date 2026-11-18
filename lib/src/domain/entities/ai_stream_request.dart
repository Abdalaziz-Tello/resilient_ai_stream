/// Immutable descriptor for an AI streaming HTTP request.
final class AiStreamRequest {
  final Uri url;
  final String method;
  final Map<String, String> headers;
  final Map<String, dynamic>? body;

  /// Completes when this request must be aborted. Custom transports should
  /// connect this signal to their HTTP client's per-request cancellation API.
  final Future<void>? cancelled;

  const AiStreamRequest({
    required this.url,
    this.method = 'POST',
    this.headers = const {},
    this.body,
    this.cancelled,
  });

  AiStreamRequest withCancellation(Future<void> signal) => AiStreamRequest(
        url: url,
        method: method,
        headers: headers,
        body: body,
        cancelled: signal,
      );

  /// Returns a new [AiStreamRequest] with additional resumption or auth headers.
  AiStreamRequest copyWithExtraHeaders(Map<String, String> extraHeaders) {
    return AiStreamRequest(
      url: url,
      method: method,
      headers: {...headers, ...extraHeaders},
      body: body,
      cancelled: cancelled,
    );
  }
}
