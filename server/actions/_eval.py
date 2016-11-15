
### import

import logging
from mqks.server.config import log
from mqks.server.lib import state

### eval action

def _eval(request):
    """
    Eval action

    @param request: adict(
        data: str - "len(state.queues)", "log.setLevel(logging.DEBUG)", etc.
        response: callable,
        ...
    )
    """
    request.response(eval(request.data))
