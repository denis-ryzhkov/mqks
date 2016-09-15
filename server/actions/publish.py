
### import

from mqks.server.lib import state

### publish action

def publish(request):
    """
    Publish action

    @param request: adict(
        id: str,
        data: str - "{event} {data}",
        ...
    )
    """
    event, data = request.data.split(' ', 1)
    _publish(request.id, event, data)

### publish

def _publish(request_id, event, data):
    """
    Publish

    @param request_id: str
    @param event: str
    @param data: str
    """
    msg = '{} event={} {}'.format(request_id, event, data)
    state.published += 1
    for queue in state.queues_by_events[event]:
        state.queues[queue].put(msg)
        state.queued += 1
