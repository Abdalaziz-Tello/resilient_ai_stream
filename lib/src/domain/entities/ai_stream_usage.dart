/// Token accounting reported by a provider (any field may be unknown).
final class AiStreamUsage {
  final int? inputTokens;
  final int? outputTokens;
  final int? cachedInputTokens;
  final int? reasoningTokens;
  final int? _totalTokens;

  const AiStreamUsage({
    this.inputTokens,
    this.outputTokens,
    this.cachedInputTokens,
    this.reasoningTokens,
    int? totalTokens,
  }) : _totalTokens = totalTokens;

  /// Provider-reported total, or input + output when both are known.
  int? get totalTokens =>
      _totalTokens ??
      (inputTokens != null && outputTokens != null
          ? inputTokens! + outputTokens!
          : null);

  /// Parses Claude (`input_tokens`/`output_tokens`), OpenAI Chat Completions
  /// (`prompt_tokens`/`completion_tokens`) and Responses-style usage objects.
  factory AiStreamUsage.fromJson(Map<String, dynamic> json) {
    int? asInt(Object? v) => v is num ? v.toInt() : null;
    Map<String, dynamic>? map(Object? v) =>
        v is Map<String, dynamic> ? v : null;
    final promptDetails =
        map(json['prompt_tokens_details']) ?? map(json['input_tokens_details']);
    final outputDetails = map(json['completion_tokens_details']) ??
        map(json['output_tokens_details']);
    return AiStreamUsage(
      inputTokens: asInt(json['input_tokens'] ?? json['prompt_tokens']),
      outputTokens: asInt(json['output_tokens'] ?? json['completion_tokens']),
      totalTokens: asInt(json['total_tokens']),
      cachedInputTokens: asInt(
          json['cache_read_input_tokens'] ?? promptDetails?['cached_tokens']),
      reasoningTokens: asInt(outputDetails?['reasoning_tokens']),
    );
  }

  /// Overlays the known fields of [other] on top of this usage.
  AiStreamUsage merge(AiStreamUsage other) => AiStreamUsage(
        inputTokens: other.inputTokens ?? inputTokens,
        outputTokens: other.outputTokens ?? outputTokens,
        cachedInputTokens: other.cachedInputTokens ?? cachedInputTokens,
        reasoningTokens: other.reasoningTokens ?? reasoningTokens,
        totalTokens: other._totalTokens,
      );

  @override
  String toString() =>
      'AiStreamUsage(input: $inputTokens, output: $outputTokens, total: $totalTokens)';
}
