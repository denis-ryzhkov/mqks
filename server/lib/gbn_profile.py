
### import

from gbn import gbn, gbn_attach, gbn_detach, gbn_report_and_reset
from gevent import spawn, wait
from gevent.event import AsyncResult
from uqid import dtid

from mqks.server.config import config, WORKERS
from mqks.server.lib import state
from mqks.server.lib.workers import at_all_workers, at_request_worker

### const

STEP_FORMAT = '{step},{sum:.6f},{min:.6f},{max:.6f},{wall_sum:.6f},{wall_min:.6f},{wall_max:.6f},{calls},{switches}'  # No "avg" to reduce traffic. avg=sum/calls.
STEP_FIELDS_SEP = ','
STEPS_SEP = ';'

### enable

def enable():
    """
    Enable gbn profiler at all workers.
    """
    request = dict(id='gbn')
    _gbn_enable(request)

@at_all_workers
def _gbn_enable(request):
    """
    @param request: dict - defined in "on_request"
    """
    _enable_local()

def _enable_local():
    gbn_attach()
    if not state.gbn_greenlet:
        state.gbn_greenlet = spawn(gbn_report_and_reset, each=config['gbn_seconds'], log=_on_gbn_report, step_format=STEP_FORMAT, steps_separator=STEPS_SEP)

def _on_gbn_report(gbn_profile):
    """
    Save gbn profile of one worker to its state.

    @param gbn_profile: str
    """
    state.gbn_profile = gbn_profile

### disable

def disable():
    """
    Disable gbn profiler at all workers.
    """
    request = dict(id='gbn')
    _gbn_disable(request)

@at_all_workers
def _gbn_disable(request):
    """
    @param request: dict - defined in "on_request"
    """
    gbn_detach()
    if state.gbn_greenlet:
        state.gbn_greenlet.kill()
        state.gbn_greenlet = None

### get

def get():
    """
    Get gbn profile, aggregated from all workers.

    @return str - aggregated gbn profile, empty on timeout.
    """
    wall = gbn('gbn_profile.get')
    state.gbn_profiles = [AsyncResult() for _ in xrange(WORKERS)]

    request = dict(id='gbn', worker=state.worker)
    _gbn_get(request)

    ready = wait(state.gbn_profiles, timeout=config['block_seconds'])
    if len(ready) < WORKERS:
        gbn(wall=wall)
        return ''

    _seconds_sum = {}
    _seconds_min = {}
    _seconds_max = {}
    _wall_sum = {}
    _wall_min = {}
    _wall_max = {}
    _calls = {}
    _switches = {}

    for worker in xrange(WORKERS):
        lines = state.gbn_profiles[worker].get()
        if not lines:
            continue

        for line in lines.split(STEPS_SEP):
            step, seconds_sum, seconds_min, seconds_max, wall_sum, wall_min, wall_max, calls, switches = line.split(STEP_FIELDS_SEP)

            _seconds_sum[step] = _seconds_sum.get(step, 0) + float(seconds_sum)
            _seconds_min[step] = min(_seconds_min[step], float(seconds_min)) if step in _seconds_min else float(seconds_min)
            _seconds_max[step] = max(_seconds_max[step], float(seconds_max)) if step in _seconds_max else float(seconds_max)

            _wall_sum[step] = _wall_sum.get(step, 0) + float(wall_sum)
            _wall_min[step] = min(_wall_min[step], float(wall_min)) if step in _wall_min else float(wall_min)
            _wall_max[step] = max(_wall_max[step], float(wall_max)) if step in _wall_max else float(wall_max)

            _calls[step] = _calls.get(step, 0) + int(calls)
            _switches[step] = _switches.get(step, 0) + int(switches)

    gbn_profile = STEPS_SEP.join(STEP_FORMAT.format(
        step=step,
        sum=seconds_sum,
        min=_seconds_min[step],
        max=_seconds_max[step],
        wall_sum=_wall_sum[step],
        wall_min=_wall_min[step],
        wall_max=_wall_max[step],
        calls=_calls[step],
        switches=_switches[step],
    ) for step, seconds_sum in sorted(_seconds_sum.iteritems(), key=lambda x: -x[1]))

    gbn(wall=wall)
    return gbn_profile

@at_all_workers
def _gbn_get(request):
    """
    @param request: dict - defined in "on_request"
    """
    _gbn_set(request, str(state.worker), state.gbn_profile)

@at_request_worker
def _gbn_set(request, worker, gbn_profile):
    """
    @param request: dict - defined in "on_request"
    @param worker: str(int)
    @param gbn_profile: str
    """
    state.gbn_profiles[int(worker)].set(gbn_profile)
