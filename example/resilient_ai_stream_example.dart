import 'dart:convert';
import 'package:resilient_ai_stream/resilient_ai_stream.dart';

/// Offline demonstration of a backend that replays the same generation with
/// unique SSE IDs. Direct provider APIs may not support this resume contract.
Future<void> main() async {
  var attempt = 0;
  const first = 'id: 1\ndata: {"text":"مرحبا "}\n\n';
  const rest =
      'id: 2\ndata: {"text":"بك!","sources":[{"id":"doc_1","title":"دليل الموظف"}]}\n\n'
      'id: 3\ndata: {"done":true}\n\n';
  final client = ResilientAiStreamClient.claudeWithCustomRag(
    transport: CustomCallbackTransport(onOpenStream: (request) async {
      attempt++;
      print(
          'Attempt $attempt; resume cursor: ${request.headers['Last-Event-ID']}');
      // Attempt 1 ends unexpectedly. Attempt 2 replays event 1 before continuing.
      return Stream<List<int>>.value(
          utf8.encode(attempt == 1 ? first : first + rest));
    }),
    recovery: StreamRecovery.resume,
    retryPolicy: const ExponentialBackoffPolicy(initialDelay: Duration.zero),
    uiCoalesceInterval: Duration.zero,
  );
  try {
    await for (final state in client.stream(AiStreamRequest(
      url: Uri.parse('https://offline.example/stream'),
    ))) {
      print('${state.runtimeType}: ${state.snapshot.text}');
      if (state is AiStreamCompleted) {
        print(
            'Sources: ${state.snapshot.citations.map((c) => c.title).join(', ')}');
      }
      if (state is AiStreamFailed) print(state.failure.message);
    }
  } finally {
    await client.dispose();
  }
}
