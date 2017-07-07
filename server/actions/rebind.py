
### import

from gbn import gbn
from mqks.server.config import config
from mqks.server.lib import state
from mqks.server.lib.workers import at_all_workers, respond
import re

### rebind action

def rebind(request):
    """
    Rebind action

    @param request: dict - defined in "on_request" with (
        data: str - "{request_id} rebind {queue} \
            [{event} ... {event}] \
            [--remove {event} ... {event}] \
            [--remove-mask {event_mask} ... {event_mask}] \
            [--add {event} ... {event}]"
    )
    """
    queue, data = request['data'].split(' ', 1)
    _rebind(request, queue, data)

### rebind command

@at_all_workers
def _rebind(request, queue, data):
    """
    Rebind command

    @param request: dict - defined in "on_request"
    @param queue: str
    @param data: str - "request['data']" without "queue" part. Parsed here, not in "rebind" action to avoid double parsing in "command protocol".
    """

    ### parse

    wall = gbn('_rebind.parse')
    args = dict(replace=[], remove=[], remove_mask=[], add=[])
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

    ### remove all

    if not found_event:
        wall = gbn('_rebind.remove_all', wall=wall)
        for event in state.events_by_queues.pop(queue, ()):
            _unbind(queue, event)

    else:
        events = state.events_by_queues.get(queue)

        ### replace

        if args['replace']:
            wall = gbn('_rebind.replace', wall=wall)
            new_events = set(args['replace'])

            if events:
                for event in events - new_events:
                    _unbind(queue, event)
                for event in new_events - events:
                    _bind(queue, event)

            else:
                for event in args['replace']:
                    _bind(queue, event)

            events = state.events_by_queues[queue] = new_events

        ### remove

        if args['remove'] and events:
            wall = gbn('_rebind.remove', wall=wall)

            for event in args['remove']:
                _unbind(queue, event)

            events.difference_update(args['remove'])
            if not events:
                state.events_by_queues.pop(queue, None)

        ### remove mask

        if args['remove_mask'] and events:
            wall = gbn('_rebind.remove_mask', wall=wall)
            key = tuple(args['remove_mask'])
            regexp = state.remove_mask_cache.get(key)

            if not regexp:
                regexp = state.remove_mask_cache[key] = re.compile('|'.join(
                    '[^.]*'.join(
                        re.escape(part) for part in mask.split('*')
                    ) + '$' for mask in args['remove_mask']
                ))
                if len(state.remove_mask_cache) > config['remove_mask_cache_limit']:
                    state.remove_mask_cache.clear()

            for event in [event for event in events if '.' in event and regexp.match(event)]:
                events.discard(event)
                _unbind(queue, event)

            if not events:
                state.events_by_queues.pop(queue, None)

        ### add

        if args['add']:
            wall = gbn('_rebind.add', wall=wall)

            for event in args['add']:
                _bind(queue, event)

            if events:
                events.update(args['add'])
            else:
                events = state.events_by_queues[queue] = set(args['add'])

    ### confirm

    gbn(wall=wall)
    if request['confirm'] and request['worker'] == state.worker:
        respond(request)

### _bind, _unbind

def _bind(queue, event):
    """
    Bind queue to event by updating queues set().
    Please update events set() outside.

    @param queue: str
    @param event: str
    """
    queues = state.queues_by_events.get(event)
    if queues:
        queues.add(queue)
    else:
        state.queues_by_events[event] = set((queue, ))

def _unbind(queue, event):
    """
    Unbind queue from event by updating queues set().
    Please update events set() outside.

    @param queue: str
    @param event: str
    """
    queues = state.queues_by_events.get(event)
    if queues:
        queues.discard(queue)
        if not queues:
            state.queues_by_events.pop(event, None)
