import 'dart:convert';
import '../../domain/contracts/json_healer.dart';
import 'incremental_json_healer.dart';

/// Stateless [JsonHealer] built on [IncrementalJsonHealer]'s lexical machine.
/// For streaming use, prefer [IncrementalJsonHealer] directly (the client does
/// when no custom healer is supplied) so that each token is scanned only once.
final class DefaultPartialJsonHealer implements JsonHealer {
  const DefaultPartialJsonHealer();

  @override
  Map<String, dynamic>? tryParsePartialObject(String partialJson) {
    final trimmed = partialJson.trim();
    if (trimmed.isEmpty || !trimmed.startsWith('{')) return null;

    try {
      final decoded = jsonDecode(trimmed);
      if (decoded is Map<String, dynamic>) return decoded;
    } catch (_) {}

    return (IncrementalJsonHealer()..append(trimmed)).preview;
  }

  @override
  String healJsonString(String truncatedJson) =>
      (IncrementalJsonHealer()..append(truncatedJson)).healed;
}
