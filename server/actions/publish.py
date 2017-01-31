
### import

from mqks.server.lib import state
from mqks.server.lib.workers import at_queues_batch_worker, respond
import time

### publish action

def publish(request):
    """
    Publish action

    @param request: adict - defined in "on_request"
    """
    state.published += 1
    event, data = request.data.split(' ', 1)
    msg = '{} event={} {}'.format(request.id, event, data)

    queues = state.queues_by_events.get(event)
    if queues:
        _put_to_queues(request, queues, msg)

    if request.confirm:
        respond(request)  # Once.

### put to queues command

@at_queues_batch_worker
def _put_to_queues(request, queues_batch, msg):
    """
    Put to queues command

    @param request: adict - defined in "on_request"
    @param queues_batch: str - a space-separated sublist of "queues" passed to "_put_to_queues", see "at_queues_batch_worker" and "command protocol"
    @param msg: str
    """
    for queue in queues_batch.split(' '):
        queue = state.queues.get(queue)
        if queue:
            queue.put(msg)
            state.queued += 1
        time.sleep(0)
