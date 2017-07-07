
### import

from critbot import crit
import logging
from mqks.server.config import config, log
from mqks.server.actions.rebind import _rebind
from mqks.server.lib import state
from mqks.server.lib.workers import at_queue_worker, respond, verbose

### delete queue action

def delete_queue(request):
    """
    Delete queue action

    @param request: dict - defined in "on_request" with (
        data: str - "{queue}",
    )
    """
    queue = request['data']
    _delete_queue(request, queue)

### delete queue command

@at_queue_worker
def _delete_queue(request, queue):
    """
    Delete queue command

    @param request: dict - defined in "on_request"
    @param queue: str
    """

    if log.level == logging.DEBUG or config['grep']:
        verbose('w{}: deleting queue {}'.format(state.worker, queue))

    confirm = request['confirm']
    request['confirm'] = False  # To avoid double confirm.

    _rebind(request, queue, '')

    for consumer_id, queue_of_consumer in state.queues_by_consumer_ids.items():
        if queue_of_consumer == queue:
            _delete_consumer_here(request, queue, consumer_id)

    state.queues.pop(queue, None)
    state.queues_to_delete_when_unused.pop(queue, None)

    queue_used = state.queues_used.pop(queue, None)
    if queue_used:
        queue_used.set()  # Cancel "_wait_used_or_delete_queue" greenlet.

    if log.level == logging.DEBUG or config['grep']:
        verbose('w{}: deleted queue {}'.format(state.worker, queue))

    if confirm:
        respond(request)

### wait used or delete queue

def _wait_used_or_delete_queue(client, queue, seconds):
    """
    Wait some seconds for queue to be used by consumers, else delete queue.

    @param queue: client
    @param queue: str
    @param seconds: float
    """
    try:
        queue_used = state.queues_used.get(queue)
        if queue_used is None or not queue_used.wait(seconds):
            request = dict(id='delete_queue_when_unused', client=client, worker=state.worker, confirm=False)
            _delete_queue(request, queue)

    except Exception:
        crit()

### anti-loop import

from mqks.server.actions.delete_consumer import _delete_consumer_here
