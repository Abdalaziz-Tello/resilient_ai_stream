import 'dart:async';

/// Schedules [callback] to run once, at the start of the next UI frame.
///
/// In Flutter, pass `(cb) => WidgetsBinding.instance.scheduleFrameCallback((_) => cb())`
/// (and call `scheduleFrame()` if needed) to get genuinely frame-synced updates.
typedef FrameScheduler = void Function(void Function() callback);

/// Coalesces bursts of values so only the latest one is emitted per window.
///
/// By default the window is a [Timer] of [interval]. When a [frameScheduler]
/// is supplied, emission is instead aligned to the host's frame callback and
/// [interval] is ignored. A zero interval without a scheduler emits every value.
final class TimerCoalescer<T> {
  final Duration interval;
  final void Function(T value) onEmit;
  final FrameScheduler? frameScheduler;

  Timer? _timer;
  T? _latestPending;
  bool _hasPending = false;
  bool _frameRequested = false;
  bool _isDisposed = false;

  TimerCoalescer({
    required this.interval,
    required this.onEmit,
    this.frameScheduler,
  });

  void schedule(T value, {bool forceImmediate = false}) {
    if (_isDisposed) return;

    if (forceImmediate ||
        (frameScheduler == null && interval == Duration.zero)) {
      _timer?.cancel();
      _timer = null;
      _hasPending = false;
      _latestPending = null;
      onEmit(value);
      return;
    }

    _latestPending = value;
    _hasPending = true;

    final scheduler = frameScheduler;
    if (scheduler != null) {
      if (_frameRequested) return;
      _frameRequested = true;
      scheduler(() {
        _frameRequested = false;
        _flush();
      });
      return;
    }
    _timer ??= Timer(interval, () {
      _timer = null;
      _flush();
    });
  }

  void _flush() {
    if (_hasPending && !_isDisposed) {
      final toSend = _latestPending as T;
      _hasPending = false;
      _latestPending = null;
      onEmit(toSend);
    }
  }

  void dispose() {
    _isDisposed = true;
    _timer?.cancel();
    _timer = null;
  }
}
