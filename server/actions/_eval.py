
### import

import logging
from mqks.server.config import config, log
from mqks.server.lib import state
from mqks.server.lib.profile import stop_gprofiler
from mqks.server.lib.workers import at_worker_sent_to, respond, send_to_worker

### eval action

def _eval(request):
    """
    Eval action

    @param request: adict - defined in "on_request" with (
        data: str - "len(state.queues)", "stop_gprofiler()", "--worker=0 log.setLevel(logging.DEBUG)", etc - see "stats.py"
        ...
    )
    """
    code = request.data

    if code.startswith('--worker='):
        worker, code = code.split(' ', 1)
        worker = int(worker.split('=')[1])
    else:
        worker = state.worker

    request.instant = True
    send_to_worker(worker, '_eval_', request, (code, ))

### eval command

@at_worker_sent_to
def _eval_(request, code):
    """
    Eval command

    @param request: adict - defined in "on_request"
    @param code: str
    """
    respond(request, str(eval(code)))
