
### import

from mqks.server.lib import state

### ack action

def ack(request):
    """
    Ack action
    @param request: adict(
        data: str - "{consumer_id} {msg_id}" or "{consumer_id} --all",
        ...
    )
    """
    consumer_id, msg_id = request.data.split(' ', 1)
    if msg_id == '--all':
        _ack_all(consumer_id)
    else:
        _ack(consumer_id, msg_id)

### ack

def _ack(consumer_id, msg_id):
    """
    Ack
    @param consumer_id: str
    @param msg_id: str
    """
    state.messages_by_consumer_ids[consumer_id].pop(msg_id, None)

### ack all

def _ack_all(consumer_id):
    """
    Ack all
    @param consumer_id: str
    """
    state.messages_by_consumer_ids.pop(consumer_id, {}).clear()
