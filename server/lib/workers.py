
### import

from collections import defaultdict
from critbot import crit
from gbn import gbn
from gevent import socket, spawn
from gevent.event import Event
from gevent.queue import Queue
from gevent.server import StreamServer
import logging
from socket import AF_INET, AF_UNIX
import os
import sys
import time
from uqid import dtid

from mqks.server.config import config, log, WORKERS
from mqks.server.lib import state
from mqks.server.lib.log import verbose
from mqks.server.lib.sockets import get_listener

### get_worker

def get_worker(item):
    """
    Find out which worker serves this item, e.g. queue name.
    Items are almost uniformly distributed by workers using hash of item.

    @param item: hashable
    @return int
    """
    return hash(item) % WORKERS

### at_queues_batch_worker

def at_queues_batch_worker(func):
    """
    Splits the list of queues mentioned in arguments of decorated function to batches and routes each batch to its worker.
    Each batch is space-joined, see "command protocol".

    @param func: function(
        request: dict - defined in "on_request",
        queues: list(str),
        ...
    )
    """
    func_name = func.__name__
    state.funcs[func_name] = func

    def routing_func(request, queues, *args):
        batches = defaultdict(list)
        for queue in queues:
            batches[get_worker(queue)].append(queue)
        for worker, queues_batch in batches.iteritems():
            send_to_worker(worker, func_name, request, (' '.join(queues_batch), ) + args)

    routing_func.__name__ = func_name
    return routing_func

### at_request_worker

def at_request_worker(func):
    """
    Routes decorated function to "request['worker']" mentioned in arguments of this function.

    @param func: function(
        request: dict - defined in "on_request",
        ...
    )
    """
    func_name = func.__name__
    state.funcs[func_name] = func

    def routing_func(request, *args):
        send_to_worker(request['worker'], func_name, request, args)

    routing_func.__name__ = func_name
    return routing_func

### at_all_workers

def at_all_workers(func):
    """
    Routes decorated function to all workers.

    @param func: function(
        request: dict - defined in "on_request",
        ...
    )
    """
    func_name = func.__name__
    state.funcs[func_name] = func

    def routing_func(request, *args):
        for worker in xrange(WORKERS):
            send_to_worker(worker, func_name, request, args)

    routing_func.__name__ = func_name
    return routing_func

### at_worker_sent_to

def at_worker_sent_to(func):
    """
    Just registers the function name that can be used in "send_to_worker".

    @param func: function(
        request: dict - defined in "on_request",
        ...
    )
    """
    state.funcs[func.__name__] = func
    return func

### workers_connector

def workers_connector():
    """
    Connects and reconnects to other workers.
    """
    while 1:
        try:
            for worker in xrange(state.worker + 1, WORKERS): # Avoids racing when two workers connect each other at the same time.
                if worker in state.socks_by_workers:
                    continue

                host, port_for_workers, _ = config['workers'][worker].split(':')
                family = AF_UNIX if host == config['host'] else AF_INET
                addr = os.path.join(config['unix_sock_dir'], 'w{}'.format(worker)) if family == AF_UNIX else (host, port_for_workers)

                sock = socket.socket(family=family)
                try:
                    sock.connect(addr)
                except Exception as e:
                    log.debug('w{}: failed to connect to w{} at {}, error: {}'.format(state.worker, worker, addr, e))
                    sock.close()
                    continue

                state.socks_by_workers[worker] = sock
                log.debug('w{}: connected to w{} at {}'.format(state.worker, worker, addr))
                sock.sendall(config['workers'][state.worker] + '\n')
                spawn(on_worker_connected, sock, worker=worker)

            if len(state.socks_by_workers) == WORKERS - 1 and not state.server_for_clients:
                state.server_for_clients = StreamServer(get_listener(config['port_for_clients']), on_client_connected)
                state.server_for_clients.start()

        except Exception:
            crit(also=dict(worker=state.worker))

        time.sleep(config['block_seconds'])

### on_worker_connected

def on_worker_connected(sock, addr=None, worker=None):
    """
    Worker connection handler.

    @param sock: gevent._socket2.socket
    @param addr: tuple(host: str, port: int)|None - Set when called from "server_for_workers".
    @param worker: int|None - Set when callled from "workers_connector".
    """
    data = None
    try:
        f = sock.makefile('r')

        if worker is None:
            if not addr:
                addr = sock.getsockname()
            log.debug('w{}: w{} from {} connected'.format(state.worker, worker, addr))

            data = f.readline().rstrip()
            try:
                worker = config['workers'].index(data)
            except ValueError:
                log.error('w{}: w{} from {} identified as unknown "{}"'.format(state.worker, worker, addr, data))
            else:
                log.debug('w{}: w{} from {} identified as known {}'.format(state.worker, worker, addr, data))
                state.socks_by_workers[worker] = sock

        if worker is not None:

            if worker not in state.commands_to_workers:
                state.commands_to_workers[worker] = Queue()
                spawn(commands_sender, worker)

            while 1:
                data = f.readline()
                if data == '':
                    break

                data = data.rstrip('\r\n')  # Don't strip '' and '\t' from '\t'-separated command.
                on_command(data)
                time.sleep(0)

    except Exception as e:
        on_worker_disconnected(worker)  # With Exception details.
    else:
        on_worker_disconnected(worker)  # No Exception.

### on_worker_disconnected

def on_worker_disconnected(worker):
    """
    Handler of worker disconnect.

    @param worker: int|None
    """
    if worker is None:
        log.info('w{}: bye w{}'.format(state.worker, worker))
    else:
        state.socks_by_workers.pop(worker, None)  # "workers_connector" or "server_for_workers" will reconnect, "commands_sender" will wait for new socket.
        crit(also='w{}: lost w{}, reconnecting, error ='.format(state.worker, worker))

### send_to_worker

def send_to_worker(worker, func_name, request, args=()):
    """
    Send command to other worker or execute command locally.

    @param worker: int
    @param func_name: str
    @param request: dict - defined in "on_request"
    @param args: tuple(str)
    """

    if log.level == logging.DEBUG or config['grep']:
        verbose('w{} > w{}: {}{} for request={}'.format(state.worker, worker, func_name, args, request))

    if worker == state.worker:
        execute(func_name, request, args)

    else:
        wall = gbn('send_to_worker.other')

        # "command protocol" encodes 40x faster than default "pickle.dumps" and produces 9x smaller result:
        command = '\t'.join((func_name, request['id'], request.get('client', '-'), str(request.get('worker', -1)), str(int(request.get('confirm', False)))) + args)

        if len(command) >= config['warn_command_bytes']:
            crit('WARNING! Too big: "{}" == {} >= {} bytes'.format(command, len(command), config['warn_command_bytes']))

        state.commands_to_workers[worker].put(command)
        state.commands_put += 1

        gbn(wall=wall)

### commands_sender

def commands_sender(worker):
    """
    Sends queued commands to other worker's socket from single greenlet.

    @param worker: int
    """
    commands = state.commands_to_workers[worker]
    while 1:
        try:
            command = commands.peek()

            sock = state.socks_by_workers.get(worker)
            if sock is None:
                time.sleep(config['block_seconds'])
                continue

            wall = gbn('commands_sender')
            try:
                sock.sendall(command + '\n')
            except Exception:
                gbn(wall=wall)
                on_worker_disconnected(worker)
                continue

            commands.get()  # Safe to delete command from "commands" queue.
            gbn(wall=wall)

            time.sleep(0)

        except Exception:
            crit(also='w{}: w{}'.format(state.worker, worker))

### on_command

def on_command(command):
    """
    Handler of incoming command from other worker.
    Decodes "command protocol" and executes command.

    @param command: str
    """
    try:
        wall = gbn('on_command')
        state.commands_got += 1

        # "command protocol" decodes 23x faster than default "pickle.loads":
        parts = command.split('\t')
        func_name, request_id, client, worker, confirm = parts[:5]
        args = parts[5:]
        request = dict(id=request_id, client=client, worker=int(worker), confirm=bool(int(confirm)))

        gbn(wall=wall)
        execute(func_name, request, args)

    except Exception:
        on_error(command)

### execute

def execute(func_name, request, args):
    """
    Execute the command and handle errors.

    @param func_name: str
    @param request: dict - defined in "on_request"
    @param args: sequence(str)
    """
    try:
        wall = gbn(func_name)

        if log.level == logging.DEBUG or config['grep']:
            verbose('w{} < {}{} for request={}'.format(state.worker, func_name, args, request))

        func = state.funcs[func_name]
        func(request, *args)
        gbn(wall=wall)

    except Exception:
        on_error((func_name, request, args), request)

### on_error

def on_error(command, request=None):
    """
    Crit and try responding the error to the client.

    @param command: None|str|tuple - defined in "execute"
    @param request: None|dict - defined in "on_request"
    """
    error = command
    try:
        error_id = dtid(config['id_length'])
        error = dict(command=command, error_id=error_id, worker=state.worker)
        crit(also=error)

        if request:
            request['error_id'] = error_id
            remote_respond(request)

    except Exception:
        crit(also=error)

### anti-loop import

from mqks.server.lib.clients import on_client_connected, remote_respond
