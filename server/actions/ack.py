
### import

import logging

from mqks.server.config import config, log
from mqks.server.lib import state
from mqks.server.lib.clients import respond
from mqks.server.lib.log import verbose

### ack action

def ack(request):
    """
    Ack action

    @param request: dict - defined in "on_request" with (
        data: str - "{consumer_id} {msg_id}" or "{consumer_id} --all",
        ...
    )
    """
    consumer_id, msg_id = request['data'].split(' ', 1)

    queue = state.queues_by_consumer_ids.get(consumer_id)
    if not queue:
        if log.level == logging.DEBUG or config['grep']:
            verbose('w{}: found no queue for request={}'.format(state.worker, request))
        return

    if msg_id == '--all':
        state.messages_by_consumer_ids.pop(consumer_id, {}).clear()
    else:
        state.messages_by_consumer_ids.get(consumer_id, {}).pop(msg_id, None)

    if request['confirm']:
        respond(request)
