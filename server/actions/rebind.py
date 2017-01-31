
### import

from adict import adict
from mqks.server.lib import state
from mqks.server.lib.workers import at_all_workers, respond

### rebind action

def rebind(request):
    """
    Rebind action

    @param request: adict - defined in "on_request" with (
        data: str - "{request_id} rebind {queue} \
            [{event} ... {event}] \
            [--remove {event} ... {event}] \
            [--add {event} ... {event}] \
            [--remove-mask {event_mask} ... {event_mask}]"
    )
    """
    queue, data = request.data.split(' ', 1)
    _rebind(request, queue, data)

### rebind command

@at_all_workers
def _rebind(request, queue, data):
    """
    Rebind command

    @param request: adict - defined in "on_request"
    @param queue: str
    @param data: str - "request.data" without "queue" part. Parsed here, not in "rebind" action to avoid double parsing in "command protocol".
    """

    ### parse

    args = adict(replace=[], remove=[], add=[], remove_mask=[])
    mode = 'replace'
    found_event = False

    for part in data.split(' '):
        if part.startswith('--'):
            if part == '--remove':
                mode = 'remove'
            elif part == '--remove-mask':
                mode = 'remove_mask'
            elif part == '--add':
                mode = 'add'
            else:
                assert False, part
        elif part:
            args[mode].append(part)
            found_event = True

    ### new list of events

    if not found_event:
        events = ()  # unbind
    elif args.replace:
        events = args.replace
    else:
        events = state.events_by_queues.get(queue, ())

    events = set(events).difference(args.remove).union(args.add)

    if args.remove_mask:
        relevant_events = [event for event in events if '.' in event]
        if relevant_events:
            for mask in [mask.split('.') for mask in args.remove_mask]:
                mask_len = len(mask)
                for event in relevant_events:
                    parts = event.split('.')

                    if len(parts) != mask_len:
                        continue

                    matched = True
                    for i, part in enumerate(parts):
                        if part == mask[i] or mask[i] == '*':
                            continue
                        matched = False
                        break

                    if matched:
                        events.discard(event)

    ### unbind

    events_to_unbind = []
    events_by_queue = state.events_by_queues.get(queue, ())
    for event in events_by_queue:
        if event not in events:
            events_to_unbind.append(event)
            queues_of_event = state.queues_by_events[event]
            queues_of_event.discard(queue)
            if not queues_of_event:
                state.queues_by_events.pop(event, None)
    for event in events_to_unbind:
        events_by_queue.discard(event)
    if not events_by_queue:
        state.events_by_queues.pop(queue, None)

    ### bind

    if events:
        state.events_by_queues.setdefault(queue, set()).update(events)
        for event in events:
            state.queues_by_events.setdefault(event, set()).add(queue)

    ### confirm

    if request.confirm and request.worker == state.worker:
        respond(request)
