
### import

from gbn import gbn
from gevent import spawn
import logging

from mqks.server.config import config, log
from mqks.server.actions.reject import _reject
from mqks.server.lib import state
from mqks.server.lib.clients import respond
from mqks.server.lib.log import verbose

### delete consumer action

def delete_consumer(request):
    """
    Delete consumer action

    @param request: dict - defined in "on_request" with (
        data: str - "{consumer_id}",
    )
    """
    consumer_id = request['data']
    _delete_consumer(request, consumer_id)

### delete consumer command

def _delete_consumer(request, consumer_id):
    """
    Delete consumer command

    @param request: dict - defined in "on_request"
    @param consumer_id: str
    """

    confirm = request['confirm']
    request['confirm'] = False  # To avoid double confirm.

    for client, consumer_ids in state.consumer_ids_by_clients.items():
        # We should check all clients, not only request['client'],
        # because _delete_consumer() may be called from _delete_queue() by another client.
        consumer_ids.discard(consumer_id)
        if not consumer_ids:
            state.consumer_ids_by_clients.pop(client, None)

    state.clients_by_consumer_ids.pop(consumer_id, None)

    queue = state.queues_by_consumer_ids.pop(consumer_id, None)
    if not queue:
        return

    consumers = state.consumers_by_queues.get(queue)
    if consumers:
        consumers.pop(consumer_id, None)
        if not consumers:
            state.consumers_by_queues.pop(queue, None)

    _reject(request, queue, consumer_id, '--all')

    if queue not in state.queues_by_consumer_ids.itervalues():

        queue_used = state.queues_used.get(queue)
        if queue_used is not None:
            queue_used.clear()

        delete_queue_when_unused = state.queues_to_delete_when_unused.get(queue)
        if delete_queue_when_unused is True:
            _delete_queue(request, queue)
        elif delete_queue_when_unused is not None:
            spawn(_wait_used_or_delete_queue, request['client'], queue, seconds=delete_queue_when_unused)

    if confirm:
        respond(request)

### delete consumers

def delete_consumers(client):
    """
    Delete consumers on disconnect of client.

    @param client: str
    """
    wall = gbn('delete_consumers')
    for consumer_id in list(state.consumer_ids_by_clients.get(client, ())):
        # get() is used instead of pop() because
        # "_delete_consumer" will discard "consumer_id" from "consumer_ids" to stop "_consume_loop",
        # and once client has no consumers - it will pop().
        # list(set()) is used to avoid "Set changed size during iteration".

        if log.level == logging.DEBUG or config['grep']:
            verbose('w{}: deleting consumer {} on disconnect of client {}'.format(state.worker, consumer_id, client))

        request = dict(id='disconnect', client=client, worker=state.worker, confirm=False)
        _delete_consumer(request, consumer_id)

    gbn(wall=wall)

### anti-loop import

from mqks.server.actions.delete_queue import _delete_queue, _wait_used_or_delete_queue
