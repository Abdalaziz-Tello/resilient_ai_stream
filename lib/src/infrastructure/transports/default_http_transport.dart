// Browser: fetch + ReadableStream. Native (Dart VM / Flutter mobile+desktop):
// dart:io. Anything else: a stub that fails with a typed, explanatory error.
export 'default_http_transport_stub.dart'
    if (dart.library.js_interop) 'default_http_transport_web.dart'
    if (dart.library.io) 'default_http_transport_io.dart';
