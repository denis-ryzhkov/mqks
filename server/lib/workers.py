
### import

from collections import defaultdict
from critbot import crit
from gbn import gbn
from gevent import spawn
from gevent.event import Event
import logging
from mqks.server.config import config, log
from mqks.server.lib import state
import time
import sys
from uqid import dtid

### init-once state

_funcs = {}                            # _funcs[func_name: str] == func: function

### get_worker

def get_worker(item):
    """
    Find out which worker serves this item, e.g. queue name.
    Items are almost uniformly distributed by workers using hash of item.

    @param item: hashable
    @return int
    """
    return hash(item) % config['workers']

### at_queue_worker

def at_queue_worker(func):
    """
    Routes decorated function to the worker that serves the queue mentioned in arguments of this function.

    @param func: function(
        request: dict - defined in "on_request",
        queue: str,
        ...
    )
    """
    func_name = func.__name__
    _funcs[func_name] = func

    def routing_func(request, queue, *args):
        send_to_worker(get_worker(queue), func_name, request, (queue, ) + args)

    routing_func.__name__ = func_name
    return routing_func

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
    _funcs[func_name] = func

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
    _funcs[func_name] = func

    def routing_func(request, *args):
        send_to_worker(request['worker'], func_name, request, args)

    routing_func.__name__ = func_name
    return routing_func

### at_all_workers

def at_all_workers(func):
    """
    Routes decorated function to all other workers and executes it locally.

    @param func: function(
        request: dict - defined in "on_request",
        ...
    )
    """
    func_name = func.__name__
    _funcs[func_name] = func

    def routing_func(request, *args):
        for worker in xrange(config['workers']):
            send_to_worker(worker, func_name, request, args)

    routing_func.__name__ = func_name
    return routing_func

### at_all_workers_local_instant

def at_all_workers_local_instant(func):
    """
    Routes decorated function to all other workers.
    Local worker executes this function instantly, without sending to "commands" queue.

    @param func: function(
        request: dict - defined in "on_request",
        ...
    )
    """
    func_name = func.__name__
    _funcs[func_name] = func

    def routing_func(request, *args):

        wall = gbn(func_name)
        func(request, *args)
        gbn(wall=wall)

        for worker in xrange(config['workers']):
            if worker != state.worker:
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
    _funcs[func.__name__] = func
    return func

### update_accepted

@at_all_workers
def update_accepted(request, accepted):
    """
    Update counter of clients accepted by "request['worker']".
    Decide to stop or start accepting clients by local "state.worker".
    See also "config['accepted_diff']" description.

    @param request: dict - defined in "on_request"
    @param accepted: str(int) - "str" is required by "send_to_worker"
    """
    wall = gbn('update_accepted')

    accepted = int(accepted)
    state.accepted_by_workers[request['worker']] = accepted

    min_accepted = min(state.accepted_by_workers)
    max_accepted = max(state.accepted_by_workers)
    local_accepted = state.accepted_by_workers[state.worker]

    if local_accepted == max_accepted and max_accepted - min_accepted >= config['accepted_diff']:
        if state.is_accepting:
            state.is_accepting = False
            state.server.stop_accepting()
            log.info('w{} stopped accepting on {}'.format(state.worker, local_accepted))

    elif not state.is_accepting:
        state.is_accepting = True
        state.server.start_accepting()
        log.info('w{} started accepting on {}'.format(state.worker, local_accepted))

    gbn(wall=wall)

### suicide_all_workers

@at_all_workers
def suicide_all_workers(request):
    sys.exit(1)

### send_to_worker

def send_to_worker(worker, func_name, request, args=()):
    """
    Send command to local worker or to other worker,
    or execute "instant" command instantly.

    @param worker: int
    @param func_name: str
    @param request: dict - defined in "on_request"
    @param args: tuple(str)
    """

    if log.level == logging.DEBUG or config['grep']:
        verbose('w{} > w{}: {}{} for request={}'.format(state.worker, worker, func_name, args, request))

    if worker == state.worker:
        command = (func_name, request, args)

        if request.get('instant'):
            execute(command)
        else:
            wall = gbn('send_to_worker.local')
            state.commands.put(command)
            gbn(wall=wall)

    else:
        wall = gbn('send_to_worker.other')

        if request.get('instant'):
            func_name = '!' + func_name

        # "command protocol" encodes 40x faster than default "pickle.dumps" and produces 9x smaller result to avoid 64K bytes limit of pipe:
        command = '\t'.join((func_name, request['id'], request.get('client', '-'), str(request.get('worker', -1)), str(int(request.get('confirm', False)))) + args)
        assert len(command) <= config['bytes_per_pipe'], command

        try:
            state.worker_writers[worker].put(command)
            state.commands_put += 1

        except OSError:
            if not state.is_suiciding:
                state.is_suiciding = True
                request = dict(id='on_lost_worker')
                suicide_all_workers(request)
                crit()

        gbn(wall=wall)

### merger

def merger(reader):
    """
    Example:
    * "worker1" writes a command for "worker2" into "worker1.writer2" handle.
    * "worker1.writer2" handle sends this command to "worker2.reader1" handle.
    * Greenlet "worker2.merger1":
    ** Reads this command from "worker2.reader1" handle.
    ** "instant" command is executed instantly,
    ** Or puts this normal command into "worker2.commands" queue.
    * See what happens next in "executor".

    worker1 --command--> worker1.writer2 --> worker2.reader1 --> worker2.merger1 -->
                                                                                     worker2.commands --> worker2.executor
    worker3 --command--> worker3.writer2 --> worker2.reader3 --> worker2.merger3 -->

    @param reader: gipc._GIPCReader
    reader.get() == command: tuple - defined in "execute"
    """
    while 1:
        command = None
        try:
            command = reader.get()

            wall = gbn('merger')
            state.commands_got += 1

            # "command protocol" decodes 23x faster than default "pickle.loads":
            parts = command.split('\t')
            func_name, request_id, client, worker, confirm = parts[:5]
            request = dict(id=request_id, client=client, worker=int(worker), confirm=bool(int(confirm)))

            if func_name[0] == '!':
                func_name = func_name[1:]
                request['instant'] = True
                command = (func_name, request, parts[5:])
                gbn(wall=wall)
                execute(command)
            else:
                command = (func_name, request, parts[5:])
                state.commands.put(command)
                gbn(wall=wall)

            time.sleep(0)

        except EOFError:
            return

        except Exception:
            on_error(command)

### executor

def executor():
    """
    Reads commands from "commands" queue and executes them sequentially to avoid racing.
    """
    while 1:
        command = None
        try:
            command = state.commands.get()
            execute(command)
            time.sleep(0)

        except Exception:
            on_error(command)

### execute

def execute(command):
    """
    Execute the command and handle errors.

    If command should execute at queue worker,
    but current worker is not serving this queue any more,
    then re-route the command.

    @param command: tuple(
        func_name: str,
        request: dict - defined in "on_request",
        args: tuple(str),
    )
    """
    request = None
    try:
        func_name, request, args = command
        wall = gbn(func_name)

        if log.level == logging.DEBUG or config['grep']:
            verbose('w{} < {}{} for request={}'.format(state.worker, func_name, args, request))

        func = _funcs[func_name]
        func(request, *args)
        gbn(wall=wall)

    except Exception:
        on_error(command, request)

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
        error = dict(command=command, error_id=error_id, worker=state.worker, request=request)
        crit(also=error)

        if request:
            request['error_id'] = error_id
            respond(request)

    except Exception:
        crit(also=error)

### respond

@at_request_worker
def respond(request, data=''):
    """
    Put response to local responses queue.
    Can not write to socket right here, as this function may be called from multiple "executor" greenlets.

    @param request: dict - defined in "on_request" + optional fields (
        error_id: str - should be set in "except"
    )
    @param data: str - response data, don't confuse it with "request['data']"
    """
    responses = state.responses_by_clients.get(request['client'])
    if responses:
        responses.put((request, data))

### verbose

def verbose(line):
    """
    Log a line if either DEBUG mode or "grep" matches.

    Usage to avoid useless string formatting:
        if log.level == logging.DEBUG or config['grep']:
            verbose('...'.format(...))
    """
    if log.level == logging.DEBUG:
        log.debug(line)
    elif config['grep'] in line:
        log.info(line)
