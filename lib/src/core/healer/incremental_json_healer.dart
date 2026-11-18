import 'dart:convert';

/// Stateful, append-only partial JSON healer.
///
/// The lexical state (open containers, string/escape state, pending keys and
/// bare tokens) is advanced only over **newly appended** characters, so feeding
/// a stream token by token costs O(delta) scanning work in total instead of
/// rescanning the whole buffer for every preview. Producing a decoded preview
/// still runs one native `jsonDecode` over the healed text, but the result is
/// cached until more input arrives.
///
/// Healing rules for a truncated document:
///  * unfinished strings are closed (partial `\uXXXX` escapes are dropped);
///  * an unfinished or dangling object key (`{"a":1,"b"` / `{"a":`) is removed;
///  * partial literals are completed (`tru` -> `true`) and dangling number
///    suffixes are trimmed (`1.` -> `1`, `-` is dropped);
///  * trailing commas are removed and open containers are closed.
///
/// It is still a best-effort previewer, not a validator: validate the original
/// completed JSON before acting on it.
final class IncrementalJsonHealer {
  static const int _quote = 0x22;
  static const int _backslash = 0x5C;
  static const int _openBrace = 0x7B;
  static const int _closeBrace = 0x7D;
  static const int _openBracket = 0x5B;
  static const int _closeBracket = 0x5D;
  static const int _colon = 0x3A;
  static const int _comma = 0x2C;

  final StringBuffer _buffer = StringBuffer();
  final List<int> _closers = <int>[];
  int _length = 0;

  bool _started = false;
  bool _invalid = false;

  bool _inString = false;
  bool _escaped = false;
  int _unicodeLeft = 0;
  int _escapeStart = -1;
  int _stringStart = -1;
  bool _stringIsKey = false;

  int _lastSignificant = 0;
  bool _pendingKey = false;
  int _keyStart = -1;
  int _danglingKeyStart = -1;
  int _tokenStart = -1;

  Map<String, dynamic>? _preview;
  int _previewLength = -1;

  /// Number of UTF-16 code units consumed so far.
  int get length => _length;

  /// Whether the input is not a JSON object/array document.
  bool get isInvalid => _invalid;

  /// Clears all state so the healer can be reused for a new document.
  void reset() {
    _buffer.clear();
    _closers.clear();
    _length = 0;
    _started = false;
    _invalid = false;
    _inString = false;
    _escaped = false;
    _unicodeLeft = 0;
    _escapeStart = -1;
    _stringStart = -1;
    _stringIsKey = false;
    _lastSignificant = 0;
    _pendingKey = false;
    _keyStart = -1;
    _danglingKeyStart = -1;
    _tokenStart = -1;
    _preview = null;
    _previewLength = -1;
  }

  /// Makes the healer reflect [full], consuming only the unseen suffix when
  /// [full] extends what was already consumed. A shorter input resets state.
  void sync(String full) {
    if (full.length < _length) reset();
    if (full.length > _length) append(full.substring(_length));
  }

  /// Appends [chunk] and advances the lexical state over it.
  void append(String chunk) {
    if (chunk.isEmpty) return;
    _buffer.write(chunk);
    final base = _length;
    _length += chunk.length;
    if (_invalid) return;
    for (var i = 0; i < chunk.length; i++) {
      _consume(chunk.codeUnitAt(i), base + i);
      if (_invalid) return;
    }
  }

  void _endToken() {
    if (_tokenStart >= 0) {
      _tokenStart = -1;
      _danglingKeyStart = -1;
    }
  }

  void _consume(int c, int index) {
    if (_inString) {
      if (_unicodeLeft > 0) {
        _unicodeLeft--;
      } else if (_escaped) {
        _escaped = false;
        if (c == 0x75) _unicodeLeft = 4; // \u
      } else if (c == _backslash) {
        _escaped = true;
        _escapeStart = index;
      } else if (c == _quote) {
        _inString = false;
        _lastSignificant = _quote;
        if (_stringIsKey) {
          _pendingKey = true;
          _keyStart = _stringStart;
        }
      }
      return;
    }

    // Whitespace.
    if (c == 0x20 || c == 0x0A || c == 0x0D || c == 0x09) {
      _endToken();
      return;
    }

    if (!_started) {
      if (c == _openBrace) {
        _closers.add(_closeBrace);
      } else if (c == _openBracket) {
        _closers.add(_closeBracket);
      } else {
        _invalid = true;
        return;
      }
      _started = true;
      _lastSignificant = c;
      return;
    }

    if (_closers.isEmpty) return; // Trailing content after the root value.

    switch (c) {
      case _quote:
        _endToken();
        final inObject = _closers.last == _closeBrace;
        final isKey = inObject &&
            (_lastSignificant == _openBrace || _lastSignificant == _comma);
        _inString = true;
        _stringStart = index;
        _stringIsKey = isKey;
        if (!isKey) _danglingKeyStart = -1;
      case _openBrace:
        _endToken();
        _closers.add(_closeBrace);
        _lastSignificant = c;
        _danglingKeyStart = -1;
      case _openBracket:
        _endToken();
        _closers.add(_closeBracket);
        _lastSignificant = c;
        _danglingKeyStart = -1;
      case _closeBrace:
      case _closeBracket:
        _endToken();
        if (_closers.last == c) {
          _closers.removeLast();
        } else {
          _invalid = true;
        }
        _lastSignificant = c;
        _danglingKeyStart = -1;
        _pendingKey = false;
      case _colon:
        _pendingKey = false;
        _danglingKeyStart = _keyStart;
        _lastSignificant = c;
      case _comma:
        _endToken();
        _lastSignificant = c;
        _danglingKeyStart = -1;
      default:
        if (_tokenStart < 0) _tokenStart = index;
        _lastSignificant = c;
    }
  }

  /// The text so far with the minimal suffix/cuts that make it valid JSON.
  String get healed {
    final text = _buffer.toString();
    if (_invalid || !_started) return text;
    if (_closers.isEmpty) return text;

    var s = text;
    if (_inString) {
      if (_escaped || _unicodeLeft > 0) s = s.substring(0, _escapeStart);
      if (_stringIsKey) {
        s = _stripTrailingComma(s.substring(0, _stringStart));
        return s + _closingSuffix();
      }
      s += '"';
    } else if (_pendingKey) {
      s = _stripTrailingComma(s.substring(0, _keyStart));
      return s + _closingSuffix();
    } else if (_tokenStart >= 0) {
      final fixed = _fixToken(s.substring(_tokenStart));
      s = s.substring(0, _tokenStart) + (fixed ?? '');
    }

    s = s.trimRight();
    if (s.endsWith(':') && _danglingKeyStart >= 0) {
      s = s.substring(0, _danglingKeyStart);
    }
    s = _stripTrailingComma(s);
    return s + _closingSuffix();
  }

  /// Decoded object preview, cached until more input arrives. Null when the
  /// current text cannot be repaired into a JSON object.
  Map<String, dynamic>? get preview {
    if (_previewLength == _length) return _preview;
    _previewLength = _length;
    _preview = null;
    if (_invalid || !_started) return null;
    try {
      final decoded = jsonDecode(healed);
      if (decoded is Map<String, dynamic>) _preview = decoded;
    } catch (_) {}
    return _preview;
  }

  String _closingSuffix() {
    final out = StringBuffer();
    for (var i = _closers.length - 1; i >= 0; i--) {
      out.writeCharCode(_closers[i]);
    }
    return out.toString();
  }

  static String _stripTrailingComma(String s) {
    var out = s.trimRight();
    if (out.endsWith(',')) out = out.substring(0, out.length - 1).trimRight();
    return out;
  }

  static String? _fixToken(String token) {
    for (final literal in const ['true', 'false', 'null']) {
      if (literal.startsWith(token)) return literal;
    }
    var number = token;
    while (number.isNotEmpty && '.eE+-'.contains(number[number.length - 1])) {
      number = number.substring(0, number.length - 1);
    }
    return number.isEmpty ? null : number;
  }
}
