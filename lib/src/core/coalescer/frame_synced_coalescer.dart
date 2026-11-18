import 'timer_coalescer.dart';

/// Former name of [TimerCoalescer]; it never synchronized with frames unless a
/// frame scheduler was supplied.
@Deprecated('Use TimerCoalescer.')
typedef FrameSyncedCoalescer<T> = TimerCoalescer<T>;
