/// Recovery requires an explicit contract with the backend.
enum StreamRecovery {
  /// Surface failures without automatically resending the request (default).
  none,

  /// Resend the request and discard the previous attempt's accumulated state.
  /// This can start a new generation and incur another provider charge.
  restart,

  /// Preserve state and send Last-Event-ID. The backend MUST replay the same
  /// generation with a unique, explicit SSE id on every application event.
  resume,
}
