import 'dart:io';
import 'package:resilient_ai_stream/resilient_ai_stream.dart';

/// Streams a real answer from the Claude API.
///
/// ```sh
/// ANTHROPIC_API_KEY=... dart run example/claude_live_example.dart
/// ```
///
/// Keep provider keys on a server or your own machine. A mobile or web app
/// should call your backend instead of a provider directly.
Future<void> main() async {
  final key = Platform.environment['ANTHROPIC_API_KEY'];
  if (key == null || key.isEmpty) {
    stderr.writeln('Set ANTHROPIC_API_KEY to run this example.');
    exitCode = 64;
    return;
  }
  final client = ResilientAiStreamClient(adapter: const ClaudeSseAdapter());
  var printed = 0;
  try {
    await for (final state in client.stream(AiStreamRequest(
      url: Uri.parse('https://api.anthropic.com/v1/messages'),
      headers: {'x-api-key': key, 'anthropic-version': '2023-06-01'},
      body: {
        'model': 'claude-haiku-5-5',
        'max_tokens': 300,
        'stream': true,
        'messages': [
          {'role': 'user', 'content': 'ما هي فوائد القراءة؟ أجب في جملتين.'}
        ],
      },
    ))) {
      // Print only the newly arrived part of the answer.
      final text = state.snapshot.text;
      if (text.length > printed) {
        stdout.write(text.substring(printed));
        printed = text.length;
      }
      switch (state) {
        case AiStreamCompleted(:final snapshot):
          stdout.writeln('\n\nFinished (${snapshot.finishReason}), '
              '${snapshot.usage?.totalTokens} tokens.');
        case AiStreamFailed(:final failure):
          stdout.writeln('\n\nFailed: ${failure.message}');
          exitCode = 1;
        default:
          break;
      }
    }
  } finally {
    await client.dispose();
  }
}
