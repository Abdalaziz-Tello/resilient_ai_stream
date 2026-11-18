/// Interface Segregation contract for repairing truncated JSON strings during streaming.
abstract interface class JsonHealer {
  Map<String, dynamic>? tryParsePartialObject(String partialJson);
  String healJsonString(String truncatedJson);
}
