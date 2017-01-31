
### import

import logging
from mqks.server.config import config, log
from mqks.server.lib import state
from mqks.server.lib.workers import at_queue_worker, respond, verbose

### ack action

def ack(request):
    """
    Ack action

    @param request: adict - defined in "on_request" with (
        data: str - "{consumer_id} {msg_id}" or "{consumer_id} --all",
        ...
    )
    """
    consumer_id, msg_id = request.data.split(' ', 1)
    queue = state.queues_by_consumer_ids.get(consumer_id)
    if queue:
        _ack(request, queue, consumer_id, msg_id)
    elif log.level == logging.DEBUG or config.grep:
        verbose('w{}: found no queue for request={}'.format(state.worker, request))

### ack command

@at_queue_worker
def _ack(request, queue, consumer_id, msg_id):
    """
    Ack command

    @param request: adict - defined in "on_request"
    @param queue: str
    @param consumer_id: str
    @param msg_id: str
    """

    if msg_id == '--all':
        state.messages_by_consumer_ids.pop(consumer_id, {}).clear()
    else:
        state.messages_by_consumer_ids.get(consumer_id, {}).pop(msg_id, None)

    if request.confirm:
        respond(request)
