
### import

from mqks.server.lib import state

### subscribe action

def subscribe(request):
    """
    Subscribe action
    @param request: adict(
        data: str - "{queue} {event1} {eventN}"
    )
    """
    parts = request.data.split(' ')
    queue = parts.pop(0)
    _subscribe(queue, *parts)

### _subscribe

def _subscribe(queue, *events):
    """
    Subscribe
    @param queue: str
    @param events: list(str)
    """

    for event in state.queues_by_events:
        if event not in events:
            state.queues_by_events[event].discard(queue)

    for event in events:
        state.queues_by_events[event].add(queue)
