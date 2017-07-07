
### import

import gbn
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
    state.is_suiciding = False                  # bool

    state.commands = Queue()                    # commands.get() == command: tuple() - defined in "execute"
    state.commands_put = 0                      # int
    state.commands_got = 0                      # int

    state.server = None                         # gevent.server.StreamServer - is set after "init_state()"
    state.all_ready_to_start = Event()          # gevent.event.Event - is set when *all* workers are ready to start their servers
    state.is_accepting = True                   # bool - this worker is accepting new clients
    state.accepted_by_workers = [0] * config['workers']     # accepted_by_workers[worker: int] == accepted: int
    state.socks_by_clients = {}                 # socks_by_clients[client: str] == sock: gevent._socket2.socket
    state.responses_by_clients = {}             # responses_by_clients[client: str].get() == tuple(request: dict, data: str)

    state.queues = {}                           # queues[queue: str].get() == msg: str
    state.queues_by_events = {}                 # queues_by_events[event] = set([queue: str])
    state.events_by_queues = {}                 # events_by_queues[queue] = set([event: str])

    state.consumer_ids_by_clients = {}          # consumer_ids_by_clients[client: str] == set([consumer_id: str])
    state.queues_by_consumer_ids = {}           # queues_by_consumer_ids[consumer_id: str] == queue: str
    state.messages_by_consumer_ids = {}         # messages_by_consumer_ids[consumer_id: str][msg_id: str] == msg: str

    state.queues_to_delete_when_unused = {}     # queues_to_delete_when_unused[queue: str] == delete_queue_when_unused: bool|float|int
    state.queues_used = {}                      # queues_used[queue: str] == event: gevent.event.Event

    state.remove_mask_cache = {}                # remove_mask_cache[key: str] == compiled_regexp: SRE_Pattern

    state.top_events = {}                       # top_events[event_mask: str] == published: int
    state.published = 0                         # int
    state.queued = 0                            # int
    state.consumed = 0                          # int

    state.gbn_greenlet = None                   # Greenlet - "gbn_report_and_reset()", when enabled.
    state.gbn_profile = ''                      # str - last profile, if any.
    state.gbn_profiles = []                     # gbn_profiles[worker: int] == result: AsyncResult, result.get() == gbn_profile: str

    gbn.state = dict(attached=False)            # See "gbn" package.
    gbn.seconds_sum = {}
    gbn.seconds_min = {}
    gbn.seconds_max = {}
    gbn.wall_seconds_sum = {}
    gbn.wall_seconds_min = {}
    gbn.wall_seconds_max = {}
    gbn.calls = {}
    gbn.switches = {}
    gbn.other = dict(step='OTHER')
