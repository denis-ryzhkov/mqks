
### import

from adict import adict
from time import clock, sleep
from mqks.server.config import config, log
from mqks.server.lib import state
from mqks.server.lib.workers import at_all_workers

### gprofiler

def start_gprofiler():
    """
    Start GreenletProfiler
    """

    import GreenletProfiler
    GreenletProfiler.set_clock_type('cpu')
    GreenletProfiler.start(builtins=True)

def stop_gprofiler():
    """
    Stop GreenletProfiler and save its report. Ignore later calls.

    Usage:
        ./mqks_eval 'stop_gprofiler()'
    """
    _stop_gprofiler(adict(id='stop_gprofiler'))

@at_all_workers
def _stop_gprofiler(request):
    """
    @param request: adict - required by "at_all_workers"
    """
    if not state.enable_gprofiler:
        return

    state.enable_gprofiler = False

    import GreenletProfiler
    GreenletProfiler.stop()
    stats = GreenletProfiler.get_func_stats()
    stats.save('/tmp/callgrind.out.mqksd.w{}'.format(state.worker), type='callgrind')
    # Open these files with "kcachegrind".

### gbn

from greenlet import getcurrent, settrace

class gbn(object):
    """
    Greenlet version of "bn()" profiler aka "gbn" to measure time in gevent precisely
    using "greenlet.settrace" to pause/continue counting time on switch from/return to original greenlet
    and using extended "total/count=min..avg..max" format.

    WARNING: Nested profiling is NOT supported,
    because "settrace" has only two greenlets switching from and to, no other clue.

    Usage:
        gbn.enable()

        with gbn('step1'):
            step1()  # Should NOT contain "with gbn" inside or recursively.

        log.info(gbn.report_and_reset())
    """

    def __init__(self, key):
        self.key = key

    def __enter__(self):
        state.gbn[id(getcurrent())] = (clock(), 0)

    def __exit__(self, exc_type, exc_val, exc_tb):
        start, seconds = state.gbn.pop(id(getcurrent()))
        seconds += clock() - start
        key = self.key

        if key not in state.min_seconds or state.min_seconds[key] > seconds:
            state.min_seconds[key] = seconds

        if key not in state.max_seconds or state.max_seconds[key] < seconds:
            state.max_seconds[key] = seconds

        if key not in state.total_seconds:
            state.total_seconds[key] = seconds
        else:
            state.total_seconds[key] += seconds

        if key not in state.count_calls:
            state.count_calls[key] = 1
        else:
            state.count_calls[key] += 1

    @staticmethod
    def report_and_reset():
        result = 'w{} seconds: {}'.format(state.worker, ', '.join(
            '{}={:.6f}/{}={:.6f}..{:.6f}..{:.6f}'.format(
                key,
                total_seconds,
                state.count_calls[key],
                state.min_seconds[key],
                total_seconds / state.count_calls[key],
                state.max_seconds[key],
            )
            for key, total_seconds in sorted(state.total_seconds.iteritems(), key=lambda x: -x[1])
        ))

        state.min_seconds.clear()
        state.max_seconds.clear()
        state.total_seconds.clear()
        state.count_calls.clear()
        return result

    @staticmethod
    def enable():
        settrace(_gbn_tracer)

def _gbn_tracer(event, args):
    if event == 'switch' or event == 'throw':
        origin, target = args

        if id(origin) in state.gbn:  # pause counting
            start, seconds = state.gbn[id(origin)]
            state.gbn[id(origin)] = (None, seconds + clock() - start)

        if id(target) in state.gbn:  # continue counting
            start, seconds = state.gbn[id(target)]
            state.gbn[id(target)] = (clock(), seconds)

### seconds_reporter

def seconds_reporter():
    """
    Logs profiling results from time to time and resets counters.
    """
    while 1:
        sleep(config.seconds_before_profile)
        log.info(gbn.report_and_reset())
