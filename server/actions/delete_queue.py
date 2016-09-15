
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
        data: str - "{queue}",
        ...
    )
    """
    queue = request.data
    _delete_queue(request.client, queue)

### delete queue

def _delete_queue(client, queue):
    """
    Delete queue

    @param client: str
    @param queue: str
    """

    log.debug('deleting queue {}'.format(queue))

    for event, queues_of_event in state.queues_by_events.items():
        queues_of_event.discard(queue)
        if not queues_of_event:
            state.queues_by_events.pop(event, None)

    for consumer_id, queue_of_consumer in state.queues_by_consumer_ids.items():
        if queue_of_consumer == queue:
            _delete_consumer(client, consumer_id)

    state.queues_to_delete_when_unused.pop(queue, None)
    state.queues_used.pop(queue, None)
    state.queues.pop(queue, None)

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
