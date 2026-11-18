import 'dart:convert';
import 'package:resilient_ai_stream/resilient_ai_stream.dart';
import 'package:test/test.dart';

Future<List<RawSseEvent>> parse(String data, {bool jsonLines = false}) =>
    Utf8SseTransformer(allowJsonLines: jsonLines)
        .bind(
          Stream<List<int>>.fromIterable(utf8.encode(data).map((b) => [b])),
        )
        .toList();

void main() {
  test('keeps spaces and removes exactly one space after colon', () async {
    final events = await parse('data:   hello  \n\n');
    expect(events.single.data, '  hello  ');
  });
  test('multiline data, CRLF, CR, comments and empty data fields', () async {
    final events =
        await parse(': heartbeat\r\ndata: one\r\ndata: two\r\n\r\ndata\r\r');
    expect(events.map((e) => e.data), ['one\ntwo', '']);
  });
  test('UTF-8 Arabic emoji and BOM survive every byte split', () async {
    final events = await parse('\uFEFFdata: مرحبا 👋🏽\n\n');
    expect(events.single.data, 'مرحبا 👋🏽');
  });
  test('empty event blocks reset event type', () async {
    final events = await parse('event: stale\n\ndata: next\n\n');
    expect(events.single.event, 'message');
  });
  test('IDs persist, can be reset, and reject NUL', () async {
    final events = await parse(
        'id: a\ndata: 1\n\ndata: 2\n\nid:\ndata: 3\n\nid: bad\u0000id\ndata: 4\n\n');
    expect(events.map((e) => e.id), ['a', 'a', '', '']);
    expect(events.map((e) => e.idIsExplicit), [true, false, true, false]);
  });
  test('EOF discards incomplete SSE frames', () async {
    expect(await parse('data: incomplete\n'), isEmpty);
    expect(await parse('data: incomplete'), isEmpty);
  });
  test('JSON-lines extension is opt-in', () async {
    expect(await parse('{"text":"hello"}\n'), isEmpty);
    expect((await parse('{"text":"hello"}\n', jsonLines: true)).single.event,
        'json_line');
  });
  test('stream cut in the middle of a character is an interruption', () async {
    final bytes = utf8.encode('data: مرحبا\n\ndata: بك');
    final cut = bytes.sublist(0, bytes.length - 1); // half of the last char
    final seen = <RawSseEvent>[];
    await expectLater(
      const Utf8SseTransformer()
          .bind(Stream<List<int>>.fromIterable(cut.map((b) => [b])))
          .forEach(seen.add),
      throwsA(isA<NetworkStreamFailure>()),
    );
    expect(seen.single.data, 'مرحبا');
  });
  test('invalid UTF-8 inside the stream stays a format error', () async {
    await expectLater(
      const Utf8SseTransformer()
          .bind(Stream<List<int>>.fromIterable([
            utf8.encode('data: ok'),
            [0xFF, 0xFE],
            utf8.encode('\n\n'),
          ]))
          .toList(),
      throwsA(isA<FormatException>()),
    );
  });
}
