
### import

from mqks.server.lib import state
from mqks.server.lib.clients import respond

### shortcuts

import logging

# noinspection PyUnresolvedReferences
from mqks.server.config import config, log, WORKERS

# noinspection PyUnresolvedReferences
from mqks.server.lib import gbn_profile

# noinspection PyUnresolvedReferences
from mqks.server.lib.workers import get_worker

def get_module(name):
    """
    @param name: str - E.g. "mqks.server.lib.workers"
    @return module
    """
    return __import__(name, globals(), locals(), ['object'])

### eval action

def _eval(request):
    """
    Eval action

    @param request: dict - defined in "on_request" with (
        data: str - "len(state.queues)", "--worker=0 log.setLevel(logging.DEBUG)", etc - see "stats.py"
        ...
    )
    """
    code = request['data']

    if code.startswith('--worker='):  # back-compat
        worker, code = code.split(' ', 1)
        worker = int(worker.split('=')[1])
        assert worker == state.worker, (worker, state.worker, 'New client should connect directly to --worker requested!')

    respond(request, str(eval(code)))
