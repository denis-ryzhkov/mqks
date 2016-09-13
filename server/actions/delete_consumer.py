
### import

from gevent import spawn
from mqks.server.config import log
from mqks.server.actions.reject import _reject
from mqks.server.lib import state

### delete consumer action

def delete_consumer(request):
    """
    Delete consumer action
    @param request: adict(
        client: str,
        data: str - "{consumer_id}",
        ...
    )
    """
    _delete_consumer(request.client, consumer_id=request.data)

### delete consumer

def _delete_consumer(client, consumer_id):
    """
    Delete consumer
    @param client: str
    @param consumer_id: str
    """

    state.consumer_ids_by_clients[client].discard(consumer_id)
    _reject(consumer_id, '--all')

    queue = state.queues_by_consumer_ids.pop(consumer_id, None)
    if queue in state.queues_by_consumer_ids.values():
        return

    state.queues_used[queue].clear()

    when_unused = state.queues_to_delete_when_unused.get(queue)
    if when_unused is True:
        _delete_queue(client, queue)
    elif when_unused is not None:
        spawn(_wait_used_or_delete_queue, client, queue, seconds=when_unused)

### delete consumers

def delete_consumers(client):
    """
    Delete consumers
    @param client: str
    """

    for consumer_id in list(state.consumer_ids_by_clients.get(client, {})):
        # Not ".pop()" - "_delete_consumer" needs to discard "consumer_id" first, then "reject".
        # "list()" is used to avoid "Set changed size during iteration".
        log.debug('deleting consumer {} on disconnect of client {}'.format(consumer_id, client))
        _delete_consumer(client, consumer_id)

    state.consumer_ids_by_clients.pop(client, {}).clear()

### anti-loop import

from mqks.server.actions.delete_queue import _delete_queue, _wait_used_or_delete_queue
