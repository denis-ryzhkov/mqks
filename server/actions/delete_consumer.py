
### import

from adict import adict
from gevent import spawn
import logging
from mqks.server.config import config, log
from mqks.server.actions.reject import _reject
from mqks.server.lib import state
from mqks.server.lib.workers import at_all_workers_local_instant, at_queue_worker, respond, verbose

### delete consumer action

def delete_consumer(request):
    """
    Delete consumer action

    @param request: adict - defined in "on_request" with (
        data: str - "{consumer_id}",
    )
    """
    consumer_id = request.data

    # Both worker serving the client and worker serving the queue should cleanup:
    queue = _cleanup_consumer_state(request, consumer_id)

    if queue:
        _delete_consumer(request, queue, consumer_id)
    elif log.level == logging.DEBUG or config.grep:
        verbose('w{}: found no queue for request={}'.format(state.worker, request))

### delete consumer command

@at_queue_worker
def _delete_consumer(request, queue, consumer_id):
    """
    Delete consumer command, routed to queue worker, if any.

    @param request: adict - defined in "on_request"
    @param queue: str
    @param consumer_id: str
    """
    _delete_consumer_here(request, queue, consumer_id)

def _delete_consumer_here(request, queue, consumer_id):
    """
    Delete consumer here, in current worker.

    @param request: adict - defined in "on_request"
    @param queue: str
    @param consumer_id: str
    """

    confirm = request.confirm
    request.confirm = False  # To avoid double confirm.

    # "_delete_consumer" command may be called from "_delete_queue" command, not from "delete_consumer" action,
    # so whatever worker this consumer client is connected to:
    _cleanup_consumer_state_at_all(request, consumer_id)
    # Instantly cleanup in local worker serving the queue is needed for upcoming queue usage check
    # and to avoid retry to current consumer on "_reject":
    _reject(request, queue, consumer_id, '--all')

    if log.level == logging.DEBUG or config.grep:
        verbose('w{}: queues_by_consumer_ids={}'.format(state.worker, state.queues_by_consumer_ids))

    if queue not in state.queues_by_consumer_ids.itervalues():

        queue_used = state.queues_used.get(queue)
        if queue_used is not None:
            queue_used.clear()

        delete_queue_when_unused = state.queues_to_delete_when_unused.get(queue)
        if delete_queue_when_unused is True:
            _delete_queue(request, queue)
        elif delete_queue_when_unused is not None:
            spawn(_wait_used_or_delete_queue, request.client, queue, seconds=delete_queue_when_unused)

    if confirm:
        respond(request)

### cleanup_consumer_state

def _cleanup_consumer_state(request, consumer_id):
    """
    Cleanup consumer state.

    @param request: adict - defined in "on_request"
    @param consumer_id: str
    @return str - queue name, if known.
    """
    for client, consumer_ids in state.consumer_ids_by_clients.items():
        consumer_ids.discard(consumer_id)
        if not consumer_ids:
            state.consumer_ids_by_clients.pop(client, None)
    return state.queues_by_consumer_ids.pop(consumer_id, None)

@at_all_workers_local_instant
def _cleanup_consumer_state_at_all(request, consumer_id):
    _cleanup_consumer_state(request, consumer_id)

### delete consumers

def delete_consumers(client):
    """
    Delete consumers on disconnect of client.

    @param client: str
    """

    for consumer_id in list(state.consumer_ids_by_clients.get(client, {})):
        # Not ".pop()" - "_delete_consumer" needs to discard "consumer_id" first, then "reject".
        # "list()" is used to avoid "Set changed size during iteration".

        if log.level == logging.DEBUG or config.grep:
            verbose('w{}: deleting consumer {} on disconnect of client {}'.format(state.worker, consumer_id, client))

        request = adict(id='disconnect', client=client, worker=state.worker, confirm=False)
        queue = state.queues_by_consumer_ids.get(consumer_id)
        if queue:
            _delete_consumer(request, queue, consumer_id)
        elif log.level == logging.DEBUG or config.grep:
            verbose('w{}: found no queue by consumer_id={} for request={}'.format(state.worker, consumer_id, request))

    state.consumer_ids_by_clients.pop(client, {}).clear()

### anti-loop import

from mqks.server.actions.delete_queue import _delete_queue, _wait_used_or_delete_queue
