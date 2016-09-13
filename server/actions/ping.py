
### ping action

def ping(request):
    """
    Ping action
    @param request: adict(
        data: str
        response: callable
        ...
    )
    """
    request.response(request.data)
