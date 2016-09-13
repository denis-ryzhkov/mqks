
### import

from collections import defaultdict
from gevent.event import Event
from gevent.queue import Queue

### state

queues = defaultdict(Queue)
queues_used = defaultdict(Event)
queues_by_events = defaultdict(set)
consumer_ids_by_clients = defaultdict(set)
messages_by_consumer_ids = defaultdict(dict)
queues_by_consumer_ids = dict()
queues_to_delete_when_unused = dict()
