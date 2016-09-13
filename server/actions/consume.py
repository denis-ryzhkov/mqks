
### import

from gevent.queue import Empty
from mqks.server import config
from mqks.server.lib import state

### consume action

def consume(request):
    """
    Consume action
    @param request: adict(
        id: str,
        client: str,
        data: str - "{queue} [--manual-ack]"
    )
    """
    parts = request.data.split(' ')

    _consume(
        request=request,
        client=request.client,
        consumer_id=request.id,
        queue=parts.pop(0),
        manual_ack='--manual-ack' in parts
    )

### consume

def _consume(request, client, consumer_id, queue, manual_ack=False):
    """
    Consume
    @param request: adict
    @param client: str
    @param consumer_id: str
    @param queue: str
    @param manual_ack: bool
    """
    state.queues_by_consumer_ids[consumer_id] = queue
    state.queues_used[queue].set()
    queue = state.queues[queue]

    consumer_ids = state.consumer_ids_by_clients[client]
    consumer_ids.add(consumer_id)

    while consumer_id in consumer_ids:
        try:
            data = queue.get(timeout=config.block_seconds)
        except Empty:
            continue

        if consumer_id in consumer_ids:  # Consumer could be deleted while being blocked above.
            if manual_ack:
                msg_id, _ = data.split(' ', 1)
                state.messages_by_consumer_ids[consumer_id][msg_id] = data

            request.response(data)
        else:
            queue.put(data)
