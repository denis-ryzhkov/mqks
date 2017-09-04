
### import

from gevent.event import Event
from gevent.queue import Queue

### state

worker = None                                   # int
is_suiciding = False                            # bool

server_for_workers_unix = None                  # gevent.server.StreamServer - to connect workers on the same host via UNIX domain sockets
server_for_workers_inet = None                  # gevent.server.StreamServer - to connect workers on other hosts via Internet sockets
socks_by_workers = {}                           # dict; socks_by_workers[worker: int] == sock: gevent._socket2.socket
commands_to_workers = {}                        # dict; commands_to_workers[worker: int] == commands: gevent.queue.Queue; commands.get() == command: str
commands_put = 0                                # int
commands_got = 0                                # int
funcs = {}                                      # funcs[func_name: str] == func: callable

server_for_clients = None                       # gevent.server.StreamServer
socks_by_clients = {}                           # dict; socks_by_clients[client: str] == sock: gevent._socket2.socket
responses_by_clients = {}                       # dict; responses_by_clients[client: str] == responses: gevent.queue.Queue; responses.get() == tuple(request: dict, data: str)
actions = {}                                    # dict; actions[action: str] == action: callable

queues = {}                                     # dict; queues[queue: str] == queue: gevent.queue.Queue; queue.get() == msg: str
queues_to_delete_when_unused = {}               # dict; queues_to_delete_when_unused[queue: str] == delete_queue_when_unused: bool|float|int
queues_used = {}                                # dict; queues_used[queue: str] == event: gevent.event.Event

queues_by_events = {}                           # dict; queues_by_events[event: str] = set([queue: str])
events_by_queues = {}                           # dict; events_by_queues[queue: str] = set([event: str])
remove_mask_cache = {}                          # dict; remove_mask_cache[key: str] == compiled_regexp: SRE_Pattern

consumer_ids_by_clients = {}                    # dict; consumer_ids_by_clients[client: str] == set([consumer_id: str])
clients_by_consumer_ids = {}                    # dict; clients_by_consumer_ids[consumer_id: str] == client: str

consumers_by_queues = {}                        # dict; consumers_by_queues[queue: str][consumer_id: str] == manual_ack: bool
queues_by_consumer_ids = {}                     # dict; queues_by_consumer_ids[consumer_id: str] == queue: str

messages_by_consumer_ids = {}                   # dict; messages_by_consumer_ids[consumer_id: str][msg_id: str] == msg: str

top_events = {}                                 # dict; top_events[event_mask: str] == published: int
published = 0                                   # int
queued = 0                                      # int
consumed = 0                                    # int

gbn_greenlet = None                             # gevent.greenlet.Greenlet(mqks.server.lib.gbn_profile.gbn_report_and_reset) - if enabled.
gbn_profile = ''                                # str - last profile, if any.
gbn_profiles = []                               # list; gbn_profiles[worker: int] == result: AsyncResult; result.get() == gbn_profile: str
