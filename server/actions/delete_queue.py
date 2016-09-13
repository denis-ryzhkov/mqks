
### import

from critbot import crit
from mqks.server.config import log
from mqks.server.lib import state

### delete queue action

def delete_queue(request):
    """
    Delete queue action
    @param request: adict(
        client: str,
        data: str - "{queue} [--when-unused[={seconds}]]",
        ...
    )
    """
    parts = request.data.split(' ')
    queue = parts.pop(0)

    when_unused = False
    for part in parts:
        if part == '--when-unused':
            when_unused = True
        elif part.startswith('--when-unused='):
            when_unused = float(part.replace('--when-unused=', ''))

    _delete_queue(request.client, queue, when_unused=when_unused)

### delete queue

def _delete_queue(client, queue, when_unused=False):
    """
    Delete queue
    @param client: str
    @param queue: str
    @param when_unused: bool|float|int -
        when_unused=False - Delete queue instantly.
        when_unused=True - Schedule delete when queue is unused by consumers.
        when_unused=5 - Schedule delete when queue is unused by consumers for 5 seconds.
    """

    if when_unused is False:
        log.debug('deleting queue {}'.format(queue))
        state.queues_to_delete_when_unused.pop(queue, None)
        for event in state.queues_by_events:
            state.queues_by_events[event].discard(queue)
        state.queues.pop(queue, None)
        state.queues_used.pop(queue, None)
        for consumer_id, queue_of_consumer in state.queues_by_consumer_ids.items():
            if queue_of_consumer == queue:
                _delete_consumer(client, consumer_id)
    else:
        state.queues_to_delete_when_unused[queue] = when_unused

### wait used or delete queue

def _wait_used_or_delete_queue(client, queue, seconds):
    """
    Wait some seconds for queue to be used by consumers, else delete queue.
    @param queue: client
    @param queue: str
    @param seconds: float
    """
    try:
        if not state.queues_used[queue].wait(seconds):
            _delete_queue(client, queue)

    except Exception:
        crit()

### anti-loop import

from mqks.server.actions.delete_consumer import _delete_consumer
