import 'dart:convert';
import 'package:resilient_ai_stream/resilient_ai_stream.dart';

Future<void> main() async {
  final client = ResilientAiStreamClient(
    transport: CustomCallbackTransport(
        onOpenStream: (_) async => Stream<List<int>>.value(
            utf8.encode('data: {"text":"web works"}\n\ndata: [DONE]\n\n'))),
  );
  final states = await client
      .stream(AiStreamRequest(url: Uri.parse('https://example.com')))
      .toList();
  if (states.last is! AiStreamCompleted ||
      states.last.snapshot.text != 'web works') {
    throw StateError('Web smoke failed');
  }
  await client.dispose();
  print('Web custom transport smoke passed');
}
