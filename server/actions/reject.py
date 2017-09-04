
### import

import logging

from mqks.server.config import config, log
from mqks.server.lib import state
from mqks.server.lib.clients import respond
from mqks.server.lib.log import verbose

### reject - action

def reject(request):
    """
    Reject action

    @param request: dict - defined in "on_request" with (
        data: str - "{consumer_id} {msg_id}",
        ...
    )
    """
    consumer_id, msg_id = request['data'].split(' ', 1)
    queue = state.queues_by_consumer_ids.get(consumer_id)
    if queue:
        _reject(request, queue, consumer_id, msg_id)
    elif log.level == logging.DEBUG or config['grep']:
        verbose('w{}: found no queue for request={}'.format(state.worker, request))

### reject command

def _reject(request, queue, consumer_id, msg_id):
    """
    Reject command

    @param request: dict - defined in "on_request"
    @param queue: str
    @param consumer_id: str
    @param msg_id: str
    """

    if msg_id == '--all':
        msgs = state.messages_by_consumer_ids.pop(consumer_id, {}).itervalues()

    else:
        msg = state.messages_by_consumer_ids.get(consumer_id, {}).pop(msg_id, None)
        msgs = () if msg is None else (msg, )

    queue = state.queues.get(queue)
    if queue:
        for msg in msgs:
            msg_id, props, data = msg.split(' ', 2)
            new_props = []
            found_retry = False
            for prop in props.split(','):
                name, value = prop.split('=', 1)
                if name == 'retry':
                    value = str(int(value) + 1)
                    found_retry = True
                new_props.append((name, value))
            if not found_retry:
                new_props.append(('retry', '1'))
            msg = ' '.join((msg_id, ','.join('='.join(prop) for prop in new_props), data))
            queue.put(msg)

    if request['confirm']:
        respond(request)
