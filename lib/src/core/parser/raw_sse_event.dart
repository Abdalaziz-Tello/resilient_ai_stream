/// Low-level representation of a single decoded Server-Sent Event frame.
final class RawSseEvent {
  final String? id;

  /// Whether this frame contained an id field, rather than inheriting one.
  final bool idIsExplicit;
  final String event;
  final String data;

  const RawSseEvent({
    this.id,
    this.idIsExplicit = true,
    required this.event,
    required this.data,
  });
}
