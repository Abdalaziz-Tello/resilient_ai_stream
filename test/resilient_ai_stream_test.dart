import 'dart:async';
import 'dart:convert';
import 'package:resilient_ai_stream/resilient_ai_stream.dart';
import 'package:test/test.dart';

void main() {
  group('DefaultPartialJsonHealer', () {
    const healer = DefaultPartialJsonHealer();

    test('heals truncated Arabic JSON object and array in real time', () {
      const truncated =
          '{"answer": "تستحق إجازة سنوية", "sources": [{"title": "لائحة الموارد';
      final parsed = healer.tryParsePartialObject(truncated);

      expect(parsed, isNotNull);
      expect(parsed!['answer'], equals('تستحق إجازة سنوية'));
      expect(
          (parsed['sources'] as List).first['title'], equals('لائحة الموارد'));
    });
  });

  group('Utf8SseTransformer + Sealed AiStreamState', () {
    test(
        'reassembles split 2-byte Arabic UTF-8 characters and emits sealed states',
        () async {
      // Simulate a stream that splits an Arabic word across TCP byte chunks
      const sseFrame1 = 'id: evt_1\nevent: content_block_delta\n'
          'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"مرحبا بك في قاعدة المعرفة"}}\n\n';
      const sseFrame2 = 'id: evt_2\nevent: custom_rag\n'
          'data: {"sources":[{"id":"doc_1","title":"دليل السياسات","page":12}],"done":true}\n\n';

      final fullBytes = utf8.encode(sseFrame1 + sseFrame2);
      // Split right in the middle of the multi-byte Arabic text
      final chunk1 = fullBytes.sublist(0, 95);
      final chunk2 = fullBytes.sublist(95);

      final fakeTransport = CustomCallbackTransport(
        onOpenStream: (_) async => Stream.fromIterable([chunk1, chunk2]),
      );

      final client = ResilientAiStreamClient.claudeWithCustomRag(
        transport: fakeTransport,
        uiCoalesceInterval: Duration.zero,
      );

      final states = await client
          .stream(AiStreamRequest(url: Uri.parse('https://example.com/sse')))
          .toList();

      expect(states.first, isA<AiStreamConnecting>());
      expect(states.any((s) => s is AiStreamStreaming), isTrue);
      expect(states.last, isA<AiStreamCompleted>());

      final completed = states.last as AiStreamCompleted;
      expect(completed.snapshot.text, equals('مرحبا بك في قاعدة المعرفة'));
      expect(completed.snapshot.citations.length, equals(1));
      expect(completed.snapshot.citations.first.title, equals('دليل السياسات'));
    });
  });
}
