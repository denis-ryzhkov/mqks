
### import

from mqks.server.lib import state

### reject - action

def reject(request):
    """
    Reject action
    @param request: adict(
        data: str - "{consumer_id} {msg_id}"
    )
    """
    consumer_id, msg_id = request.data.split(' ', 1)
    _reject(consumer_id, msg_id)

### _reject

def _reject(consumer_id, msg_id):
    """
    Reject
    @param consumer_id: str
    @param msg_id: str
    """

    queue = state.queues_by_consumer_ids.get(consumer_id)
    if queue is None:
        return

    queue = state.queues[queue]

    if msg_id == '--all':
        msgs = state.messages_by_consumer_ids.pop(consumer_id, {}).values()

    else:
        msg = state.messages_by_consumer_ids[consumer_id].pop(msg_id, None)
        msgs = () if msg is None else (msg, )

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
        msg = '{} {} {}'.format(msg_id, ','.join('{}={}'.format(*prop) for prop in new_props), data)
        queue.put(msg)
