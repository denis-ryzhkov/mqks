
### import

from mqks.server.lib import state

### eval action

def _eval(request):
    """
    Eval action

    @param request: adict(
        data: str - "len(state.queues)", etc.
        response: callable,
        ...
    )
    """
    request.response(eval(request.data))
