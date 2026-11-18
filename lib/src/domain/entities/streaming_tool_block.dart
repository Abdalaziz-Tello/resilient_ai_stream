/// Immutable value object representing a live tool call or structured JSON block.
final class StreamingToolBlock {
  final int index;
  final String id;
  final String name;
  final String rawPartialJson;
  final Map<String, dynamic>? healedArguments;
  final bool isComplete;

  const StreamingToolBlock({
    required this.index,
    required this.id,
    required this.name,
    this.rawPartialJson = '',
    this.healedArguments,
    this.isComplete = false,
  });

  StreamingToolBlock copyWith({
    String? id,
    String? name,
    String? rawPartialJson,
    Map<String, dynamic>? healedArguments,
    bool? isComplete,
  }) {
    return StreamingToolBlock(
      index: index,
      id: id ?? this.id,
      name: name ?? this.name,
      rawPartialJson: rawPartialJson ?? this.rawPartialJson,
      healedArguments: healedArguments ?? this.healedArguments,
      isComplete: isComplete ?? this.isComplete,
    );
  }
}
