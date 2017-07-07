
### import

from mqks.server.lib.workers import respond

### ping action

def ping(request):
    """
    Ping action

    @param request: dict - defined in "on_request"
    """
    respond(request, request['data'])
