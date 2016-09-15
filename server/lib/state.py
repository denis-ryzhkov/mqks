
### import

from collections import defaultdict
from gevent.event import Event
from gevent.queue import Queue

### state

queues = defaultdict(Queue)
queues_by_events = defaultdict(set)

consumer_ids_by_clients = defaultdict(set)
queues_by_consumer_ids = dict()
messages_by_consumer_ids = defaultdict(dict)

queues_to_delete_when_unused = dict()
queues_used = defaultdict(Event)

published = 0
queued = 0
consumed = 0
