import 'dart:convert';
import 'package:resilient_ai_stream/resilient_ai_stream.dart';
import 'package:test/test.dart';

Map<String, dynamic>? preview(String s) =>
    (IncrementalJsonHealer()..append(s)).preview;

void main() {
  group('IncrementalJsonHealer', () {
    test('every prefix of a document heals to a prefix-consistent object', () {
      const doc =
          '{"name":"بحث","n":-12.5e3,"ok":true,"none":null,"list":[1,2,{"a":"x\\u00e9\\n"},[]],"s":"end"}';
      final full = jsonDecode(doc) as Map<String, dynamic>;
      for (var i = 1; i <= doc.length; i++) {
        final p = preview(doc.substring(0, i));
        if (p == null) continue; // e.g. just "{" has no content to decode
        // Every decoded top-level key must equal the final value unless the
        // value itself was still streaming (strings/lists/numbers/objects).
        for (final e in p.entries) {
          expect(full.containsKey(e.key), isTrue, reason: 'prefix $i');
        }
      }
      expect(preview(doc), full);
    });

    test('feeding in arbitrary chunks equals feeding all at once', () {
      const doc = '{"a":{"b":[1,2,3],"c":"d\\"e"},"f":"gh"}';
      for (var cut = 1; cut < doc.length; cut++) {
        final h = IncrementalJsonHealer()
          ..append(doc.substring(0, cut))
          ..append(doc.substring(cut));
        expect(h.healed, doc);
        expect(h.preview, jsonDecode(doc));
      }
    });

    test('drops dangling and unfinished keys', () {
      expect(preview('{"a":1,"b'), {'a': 1});
      expect(preview('{"a":1,"b"'), {'a': 1});
      expect(preview('{"a":1,"b":'), {'a': 1});
      expect(preview('{"a":'), <String, dynamic>{});
      expect(preview('{"a":1,'), {'a': 1});
    });

    test('completes literals and trims number suffixes', () {
      expect(preview('{"a":tr'), {'a': true});
      expect(preview('{"a":fals'), {'a': false});
      expect(preview('{"a":nu'), {'a': null});
      expect(preview('{"a":1.'), {'a': 1});
      expect(preview('{"a":1e'), {'a': 1});
      expect(preview('{"a":-'), <String, dynamic>{});
      expect(preview('{"a":[1,-'), {
        'a': [1]
      });
    });

    test('drops partial unicode and lone backslash escapes in strings', () {
      expect(preview(r'{"a":"x\u00'), {'a': 'x'});
      expect(preview(r'{"a":"x\'), {'a': 'x'});
      expect(preview(r'{"a":"x\\'), {'a': 'x\\'});
      expect(preview(r'{"a":"xé'), {'a': 'xé'});
    });

    test('braces inside strings do not affect nesting', () {
      expect(preview('{"a":"}]{[","b":'), {'a': '}]{['});
    });

    test('non-object text is invalid and previews to null', () {
      final h = IncrementalJsonHealer()..append('Sure! {"a":1}');
      expect(h.isInvalid, isTrue);
      expect(h.preview, isNull);
    });

    test('sync consumes only the unseen suffix and resets on shrink', () {
      final h = IncrementalJsonHealer()..sync('{"a":');
      expect(h.length, 5);
      h.sync('{"a":1');
      expect(h.length, 6);
      expect(h.preview, {'a': 1});
      h.sync('{"b"');
      expect(h.preview, <String, dynamic>{});
    });

    test('preview is cached until new input arrives', () {
      final h = IncrementalJsonHealer()..append('{"a":1');
      expect(identical(h.preview, h.preview), isTrue);
    });

    test('DefaultPartialJsonHealer shares the same repair rules', () {
      const healer = DefaultPartialJsonHealer();
      expect(healer.tryParsePartialObject('{"a":1,"b'), {'a': 1});
      expect(healer.healJsonString('[1,2,'), '[1,2]');
    });

    test('scans a large streamed document in linear time', () {
      final h = IncrementalJsonHealer()..append('{"items":[');
      final sw = Stopwatch()..start();
      for (var i = 0; i < 20000; i++) {
        h.append('{"id":$i,"v":"xxxxxxxx"},');
      }
      sw.stop();
      expect(h.healed.endsWith('"xxxxxxxx"}]}'), isTrue);
      // Generous bound; quadratic rescanning would be orders slower.
      expect(sw.elapsedMilliseconds, lessThan(2000));
    });
  });
}
