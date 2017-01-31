
### import

from gevent.event import Event
from gevent.queue import Queue
from mqks.server.config import config

### before init_state

ready_to_start = 0                              # int - how many workers are ready to start

### init_state

def init_state(state, worker, worker_writers):
    """
    Initialize state of current worker.
    See also "stats.py" for descriptions.

    @param worker: int - Index of current worker.
    @param worker_writers: dict - see below.
    """

    state.worker = worker                       # worker: int
    state.worker_writers = worker_writers       # worker_writers[reading_worker: int] == writer: gipc._GIPCWriter
    state.enable_gprofiler = config.enable_gprofiler    # bool
    state.is_suiciding = False                  # bool

    state.commands = Queue()                    # commands.get() == command: tuple() - defined in "execute"
    state.commands_put = 0                      # int
    state.commands_got = 0                      # int

    state.server = None                         # gevent.server.StreamServer - is set after "init_state()"
    state.all_ready_to_start = Event()          # gevent.event.Event - is set when *all* workers are ready to start their servers
    state.is_accepting = True                   # bool - this worker is accepting new clients
    state.accepted_by_workers = [0] * config.workers    # accepted_by_workers[worker: int] == accepted: int
    state.socks_by_clients = {}                 # socks_by_clients[client: str] == sock: gevent._socket2.socket
    state.responses_by_clients = {}             # responses_by_clients[client: str].get() == tuple(request: adict, data: str)

    state.queues = {}                           # queues[queue: str].get() == msg: str
    state.queues_by_events = {}                 # queues_by_events[event] = set([queue: str])
    state.events_by_queues = {}                 # events_by_queues[queue] = set([event: str])

    state.consumer_ids_by_clients = {}          # consumer_ids_by_clients[client: str] == set([consumer_id: str])
    state.queues_by_consumer_ids = {}           # queues_by_consumer_ids[consumer_id: str] == queue: str
    state.messages_by_consumer_ids = {}         # messages_by_consumer_ids[consumer_id: str][msg_id: str] == msg: str

    state.queues_to_delete_when_unused = {}     # queues_to_delete_when_unused[queue: str] == delete_queue_when_unused: bool|float|int
    state.queues_used = {}                      # queues_used[queue: str] == event: gevent.event.Event

    state.published = 0                         # int
    state.queued = 0                            # int
    state.consumed = 0                          # int

    state.gbn = {}                              # gbn[greenlet_id] == tuple(start: float, seconds: float)
    state.min_seconds = {}                      # min_seconds[key: str] == seconds: float
    state.max_seconds = {}                      # max_seconds[key: str] == seconds: float
    state.total_seconds = {}                    # total_seconds[key: str] == seconds: float
    state.count_calls = {}                      # count_calls[key: str] == count: int
