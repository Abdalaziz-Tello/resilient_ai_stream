import 'dart:async';
import 'dart:convert';
import '../../domain/failures/ai_stream_failure.dart';
import 'raw_sse_event.dart';

/// Decodes UTF-8 across byte boundaries and interprets SSE fields without
/// trimming payload data. Incomplete SSE events at EOF are discarded.
///
/// Invalid UTF-8 inside the stream is a [FormatException] (a protocol
/// failure). A stream that merely *ends* in the middle of a multi-byte
/// character was cut off, so it is reported as a [NetworkStreamFailure].
final class Utf8SseTransformer
    extends StreamTransformerBase<List<int>, RawSseEvent> {
  /// NDJSON is an explicit extension, disabled for strict SSE streams.
  final bool allowJsonLines;
  const Utf8SseTransformer({this.allowJsonLines = false});

  @override
  Stream<RawSseEvent> bind(Stream<List<int>> stream) async* {
    final lines = _decodeUtf8(stream).transform(const LineSplitter());
    String? currentId;
    String currentEvent = '';
    bool explicitId = false;
    bool firstLine = true;
    final dataLines = <String>[];

    await for (var line in lines) {
      if (firstLine) {
        firstLine = false;
        if (line.startsWith('\uFEFF')) line = line.substring(1);
      }
      if (line.isEmpty) {
        if (dataLines.isNotEmpty) {
          yield RawSseEvent(
            id: currentId,
            idIsExplicit: explicitId,
            event: currentEvent.isEmpty ? 'message' : currentEvent,
            data: dataLines.join('\n'),
          );
        }
        dataLines.clear();
        currentEvent = '';
        explicitId = false;
        continue;
      }
      if (line.startsWith(':')) continue;
      if (allowJsonLines && (line.startsWith('{') || line.startsWith('['))) {
        yield RawSseEvent(event: 'json_line', data: line, idIsExplicit: false);
        continue;
      }
      final colon = line.indexOf(':');
      final field = colon < 0 ? line : line.substring(0, colon);
      var value = colon < 0 ? '' : line.substring(colon + 1);
      if (value.startsWith(' ')) value = value.substring(1);
      switch (field) {
        case 'id':
          if (!value.contains('\u0000')) {
            currentId = value;
            explicitId = true;
          }
        case 'event':
          currentEvent = value;
        case 'data':
          dataLines.add(value);
      }
    }
  }
}

/// Chunked UTF-8 decoding that distinguishes malformed bytes (thrown from
/// `add`) from a truncated final character (thrown from `close`).
Stream<String> _decodeUtf8(Stream<List<int>> stream) async* {
  final decoded = _StringCollector();
  final bytes = utf8.decoder.startChunkedConversion(decoded);
  await for (final chunk in stream) {
    bytes.add(chunk);
    yield* decoded.drain();
  }
  try {
    bytes.close();
  } on FormatException catch (e, st) {
    throw NetworkStreamFailure(
      message: 'Stream ended in the middle of a UTF-8 character.',
      cause: e,
      stackTrace: st,
    );
  }
  yield* decoded.drain();
}

final class _StringCollector implements Sink<String> {
  final _pending = <String>[];

  @override
  void add(String data) {
    if (data.isNotEmpty) _pending.add(data);
  }

  @override
  void close() {}

  Stream<String> drain() {
    if (_pending.isEmpty) return const Stream.empty();
    final out = List<String>.of(_pending);
    _pending.clear();
    return Stream.fromIterable(out);
  }
}
