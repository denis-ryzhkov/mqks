
### import

from gevent.queue import Empty
from mqks.server import config
from mqks.server.actions.delete_consumer import _delete_consumer
from mqks.server.lib import state

### consume action

def consume(request):
    """
    Consume action

    @param request: adict(
        id: str,
        client: str,
        data: str - "{queue} {event1} [{event2} ... {eventN}] [--delete-queue-when-unused[={seconds}]] [--manual-ack]",
        confirm: bool,
        ...
    )
    """
    queue = None
    events = []
    delete_queue_when_unused = False
    manual_ack = False

    for part in request.data.split(' '):
        if part.startswith('--'):
            if part == '--delete-queue-when-unused':
                delete_queue_when_unused = True

            elif part.startswith('--delete-queue-when-unused='):
                delete_queue_when_unused = float(part.replace('--delete-queue-when-unused=', ''))

            elif part == '--manual-ack':
                manual_ack = True

            else:
                assert False, part

        elif not queue:
            queue = part
        else:
            events.append(part)

    assert queue
    assert events

    _consume(
        request=request,
        client=request.client,
        consumer_id=request.id,
        queue=queue,
        events=events,
        delete_queue_when_unused=delete_queue_when_unused,
        manual_ack=manual_ack,
    )

### consume

def _consume(request, client, consumer_id, queue, events, delete_queue_when_unused=False, manual_ack=False):
    """
    Consume

    @param request: adict
    @param client: str
    @param consumer_id: str
    @param queue: str
    @param events: iterable(str)
    @param delete_queue_when_unused: bool|float|int
    @param manual_ack: bool
    """

    for other_client, other_consumer_ids in state.consumer_ids_by_clients.items():
        if consumer_id in other_consumer_ids:  # Possible if reconnect happened before server detected disconnect.
            _delete_consumer(other_client, consumer_id)

    for event in state.queues_by_events:
        if event not in events:
            state.queues_by_events[event].discard(queue)

    for event in events:
        state.queues_by_events[event].add(queue)

    if delete_queue_when_unused is not False:
        state.queues_to_delete_when_unused[queue] = delete_queue_when_unused

    state.queues_by_consumer_ids[consumer_id] = queue
    state.queues_used[queue].set()
    queue = state.queues[queue]

    consumer_ids = state.consumer_ids_by_clients[client]
    consumer_ids.add(consumer_id)

    if request.confirm:
        request.response()

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
            state.consumed += 1
        else:
            queue.put(data)
