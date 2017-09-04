
### import

from gbn import gbn
from gevent import spawn
from gevent.event import Event
from gevent.queue import Empty, Queue
import time

from mqks.server.config import config
from mqks.server.actions.rebind import rebind
from mqks.server.lib import state
from mqks.server.lib.clients import respond
from mqks.server.lib.workers import on_error

### consume action

def consume(request):
    """
    Consume action

    @param request: dict - defined in "on_request" with (
        data: str - "{queue} [{event} ... {event}] [--add {event} ... {event}] [--delete-queue-when-unused[={seconds}]] [--manual-ack]"
    )
    """

    ### parse

    consumer_id = request['id']
    parts = request['data'].split(' ', 1)
    queue, data = parts if len(parts) == 2 else (request['data'], '')

    events_replace = []
    events_add = []
    adding = False
    delete_queue_when_unused = False
    manual_ack = False

    for part in data.split(' '):
        if part.startswith('--'):
            if part == '--add':
                adding = True
            elif part == '--delete-queue-when-unused':
                delete_queue_when_unused = True
            elif part.startswith('--delete-queue-when-unused='):
                delete_queue_when_unused = float(part.replace('--delete-queue-when-unused=', ''))
            elif part == '--manual-ack':
                manual_ack = True
            else:
                assert False, part
        elif part:
            (events_add if adding else events_replace).append(part)

    ### stop delete_queue_when_unused, reconfigure it

    state.queues_used.setdefault(queue, Event()).set()
    update_consumers = False

    if delete_queue_when_unused is False:  # Not float/int zero.
        if state.queues_to_delete_when_unused.pop(queue, None) is not None:
            update_consumers = True
    elif state.queues_to_delete_when_unused.get(queue) != delete_queue_when_unused:
        state.queues_to_delete_when_unused[queue] = delete_queue_when_unused
        update_consumers = True

    ### consumer_ids_by_clients, clients_by_consumer_ids

    consumer_ids = state.consumer_ids_by_clients.setdefault(request['client'], set())
    consumer_ids.add(consumer_id)
    state.clients_by_consumer_ids[consumer_id] = request['client']

    ### queues_by_consumer_ids, consumers_by_queues

    state.queues_by_consumer_ids[consumer_id] = queue
    state.consumers_by_queues.setdefault(queue, {})[consumer_id] = manual_ack

    ### rebind

    rebind(request, queue, replace=events_replace, add=events_add, update_consumers=update_consumers, dont_update_consumer_id=None if adding else consumer_id)

    ### finish consume init

    queue = state.queues.setdefault(queue, Queue())
    spawn(_consume_loop, request, queue, consumer_id, consumer_ids, manual_ack)
    # "rebind" will confirm instead of "consume" when all required workers are notified.

### consume loop greenlet

def _consume_loop(request, queue, consumer_id, consumer_ids, manual_ack):
    """
    Async loop to consume messages and to send them to clients.

    @param request: dict - defined in "on_request"
    @param queue: gevent.queue.Queue
    @param consumer_id: str
    @param consumer_ids: set([str])
    @param manual_ack: bool
    """

    try:
        while consumer_id in consumer_ids:
            try:
                data = queue.get(timeout=config['block_seconds'])
            except Empty:
                continue

            if consumer_id in consumer_ids:  # Consumer could be deleted while being blocked above.
                if manual_ack:
                    wall = gbn('manual_ack')
                    msg_id, _ = data.split(' ', 1)
                    state.messages_by_consumer_ids.setdefault(consumer_id, {})[msg_id] = data
                    gbn(wall=wall)

                respond(request, data)
                state.consumed += 1
            else:
                wall = gbn('consume_back')
                queue.put(data)
                gbn(wall=wall)
                # Don't try to preserve chronological order of messages in this edge case:
                # "peek() + get()" does not fit for multiple consumers from the same queue.
                # "JoinableQueue().task_done()" is heavier and "reject()" will change order anyway.

            time.sleep(0)

    except Exception:
        on_error('_consume_loop', request)
