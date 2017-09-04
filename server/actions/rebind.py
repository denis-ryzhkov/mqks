
### import

from gbn import gbn
from gevent import spawn_later
import re

from mqks.server.config import config
from mqks.server.lib import state
from mqks.server.lib.clients import respond
from mqks.server.lib.workers import at_worker_sent_to, get_worker, send_to_worker

### rebind action

def rebind(request, queue=None, update_consumers=False, dont_update_consumer_id=None, **args):
    """
    Rebind action

    @param request: dict - defined in "on_request" with (
        data: str - "{queue} \
            [{event} ... {event}] \
            [--remove {event} ... {event}] \
            [--remove-mask {event_mask} ... {event_mask}] \
            [--add {event} ... {event}]"
    )
    # When "rebind" is called from other actions:
    @param queue: str
    @param update_consumers: bool - If there is some other need to update consumers, e.g. changing --delete-queue-when-unused.
    @param dont_update_consumer_id: str|None - No need to update consumer that initiated rebind on "consume" without "--add".
    @param args: {'replace': [], 'remove': [], 'remove-mask': [], 'add': []} - No args means remove all.
    """

    ### when called from other actions

    if queue:
        wall = None
        remove_all = not args

    ### parse

    else:
        wall = gbn('rebind.parse')
        queue, data = request['data'].split(' ', 1)
        remove_all = not data
        if not remove_all:
            args = {'replace': [], 'remove': [], 'remove-mask': [], 'add': []}
            arg = args['replace']  # Default.
            for part in data.split(' '):
                if part.startswith('--'):
                    arg = args[part[2:]]
                elif part:
                    arg.append(part)

    ### old_events

    wall = gbn('rebind.old_events', wall=wall)
    old_events = state.events_by_queues.get(queue) or set()

    ### remove_all

    if remove_all:
        wall = gbn('rebind.remove_all', wall=wall)
        remove = old_events
        add = new_events = ()

    else:

        ### replace

        if args.get('replace'):
            wall = gbn('rebind.replace', wall=wall)
            new_events = set(args['replace'])
        else:
            new_events = old_events.copy()

        ### remove

        if args.get('remove') and new_events:
            wall = gbn('rebind.remove', wall=wall)
            new_events.difference_update(args['remove'])

        ### remove mask

        if args.get('remove-mask') and new_events:
            wall = gbn('rebind.remove-mask', wall=wall)
            key = tuple(args['remove-mask'])
            regexp = state.remove_mask_cache.get(key)

            if not regexp:
                regexp = state.remove_mask_cache[key] = re.compile('|'.join(
                    '[^.]*'.join(
                        re.escape(part) for part in mask.split('*')
                    ) + '$' for mask in args['remove-mask']
                ))
                if len(state.remove_mask_cache) > config['remove_mask_cache_limit']:
                    state.remove_mask_cache.clear()

            new_events.difference_update([event for event in new_events if '.' in event and regexp.match(event)])

        ### add

        if args.get('add'):
            wall = gbn('rebind.add', wall=wall)
            new_events.update(args['add'])

        ### diff

        wall = gbn('rebind.diff', wall=wall)
        if not old_events:
            remove = ()
            add = new_events
        elif not new_events:
            remove = old_events
            add = ()
        else:  # Cases above are just optimizations of this code.
            remove = old_events - new_events
            add = new_events - old_events

    ### update_consumers

    if update_consumers or new_events != old_events:
        wall = gbn('rebind.update_consumers', wall=wall)
        consumers = state.consumers_by_queues.get(queue)
        if consumers:
            delete_queue_when_unused = state.queues_to_delete_when_unused.get(queue)
            response_data = ''.join((
                '--update ', queue,
                ' ' + ' '.join(sorted(new_events)) if new_events else '',
                '' if delete_queue_when_unused is None else ' --delete-queue-when-unused' + (
                    '' if delete_queue_when_unused is True else '={}'.format(delete_queue_when_unused)
                ),
            ))

            for consumer_id, manual_ack in consumers.iteritems():
                if consumer_id != dont_update_consumer_id:
                    consumer_client = state.clients_by_consumer_ids.get(consumer_id)
                    if consumer_client:
                        consumer_request = dict(id=consumer_id, client=consumer_client, worker=state.worker)  # Consumer clients are connected to worker of queue, processing rebind.
                        respond(consumer_request, response_data + (' --manual-ack' if manual_ack else ''))

    ### send

    if remove or add:
        wall = gbn('rebind.send', wall=wall)

        # Split by unique workers of events:
        partial_rebinds = {}
        for events in remove, add:
            is_add_index = int(events is add)  # 0=remove, 1=add
            for event in events:
                worker = get_worker(event)
                if worker == state.worker:
                    continue  # Worker of queue will get full _rebind.
                if worker not in partial_rebinds:
                    partial_rebinds[worker] = ([], [])  # Owl with square eyes: partial_remove, partial_add.
                partial_rebinds[worker][is_add_index].append(event)

        # Send partial _rebind to unique workers of events:
        for worker, (partial_remove, partial_add) in partial_rebinds.iteritems():
            send_to_worker(worker, '_rebind', request, (queue, ' '.join(partial_remove), ' '.join(partial_add)))

        # Send full _rebind to self - worker of queue:
        _rebind(request, queue, ' '.join(remove), ' '.join(add))
        # This "_rebind" will confirm instead of "rebind".

        gbn(wall=wall)

    ### no-op

    else:
        wall = gbn('rebind.no-op', wall=wall)  # Just to count percent.
        gbn(wall=wall)

        if request['confirm']:
            respond(request)

### rebind command

@at_worker_sent_to  # Not @at_all_workers. See routing in "rebind".
def _rebind(request, queue, remove, add):
    """
    Rebind command

    @param request: dict - defined in "on_request"
    @param queue: str
    @param remove: str - Space-separated list of events to remove from subscriptions of this queue 
    @param add: str - Space-separated list of events to add to subscriptions of this queue 
    """

    wall = gbn('_rebind.init')
    events = state.events_by_queues.get(queue)

    ### remove

    if remove and events:
        wall = gbn('_rebind.remove', wall=wall)
        remove = remove.split(' ')
        events.difference_update(remove)
        if not events:
            state.events_by_queues.pop(queue, None)

        for event in remove:
            queues = state.queues_by_events.get(event)
            if queues:
                queues.discard(queue)
                if not queues:
                    state.queues_by_events.pop(event, None)

    ### add

    if add:
        wall = gbn('_rebind.add', wall=wall)
        add = add.split(' ')
        if events:
            events.update(add)
        else:
            state.events_by_queues[queue] = set(add)

        for event in add:
            queues = state.queues_by_events.get(event)
            if queues:
                queues.add(queue)
            else:
                state.queues_by_events[event] = set((queue, ))

    ### confirm

    gbn(wall=wall)
    if request['confirm'] and request['worker'] == state.worker:
        spawn_later(config['rebind_confirm_seconds'], respond, request)
        # "--confirm" is used mainly in tests.
        # Add result aggregation complexity as in "gbn_profile.get()" - if needed only.
