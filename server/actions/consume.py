
### import

from gbn import gbn
from gevent import spawn
from gevent.event import Event
from gevent.queue import Empty, Queue
from mqks.server.config import config
from mqks.server.actions.rebind import _rebind
from mqks.server.lib import state
from mqks.server.lib.workers import at_queue_worker, respond, on_error
import time

### consume action

def consume(request):
    """
    Consume action

    @param request: dict - defined in "on_request" with (
        data: str - "{queue} [{event} ... {event}] [--add {event} ... {event}] [--delete-queue-when-unused[={seconds}]] [--manual-ack]"
    )
    """
    parts = request['data'].split(' ', 1)
    queue, data = parts if len(parts) == 2 else (request['data'], '')
    consumer_id = request['id']

    # Both worker serving the client and worker serving the queue should store this:
    state.consumer_ids_by_clients.setdefault(request['client'], set()).add(consumer_id)  # Required for "delete_consumers" on disconnect.
    state.queues_by_consumer_ids[consumer_id] = queue  # Required for "ack, reject, delete_consumer".

    _consume_init(request, queue, data)

### consume init command

@at_queue_worker
def _consume_init(request, queue, data):
    """
    Consume init command

    @param request: dict - defined in "on_request"
    @param queue: str
    @param data: str - "request['data']" without "queue" part. Parsed here, not in "consume" action to avoid double parsing in "command protocol".
    """

    consumer_id = request['id']
    confirm = request['confirm']

    ### parse

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

    if delete_queue_when_unused is False:  # Not float/int zero.
        state.queues_to_delete_when_unused.pop(queue, None)
    else:
        state.queues_to_delete_when_unused[queue] = delete_queue_when_unused

    ### consumer_ids_by_clients, queues_by_consumer_ids

    consumer_ids = state.consumer_ids_by_clients.setdefault(request['client'], set())
    consumer_ids.add(consumer_id)

    state.queues_by_consumer_ids[consumer_id] = queue

    ### new list of events, heavy rebind

    old_events = state.events_by_queues.get(queue, ())
    events = set(events_replace or old_events).union(events_add)
    if events != set(old_events):  # "_rebind" is a relatively heavy sync "at_all_workers", try to avoid it on reconnects.
        _rebind(request, queue, ' '.join(events))
        # "_rebind" will confirm to "request['worker']" when done - OK.
        confirm = False  # To avoid double confirm.

    ### finish consume init

    queue = state.queues.setdefault(queue, Queue())

    if confirm:
        respond(request)

    spawn(_consume_loop, request, queue, consumer_id, consumer_ids, manual_ack)

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
