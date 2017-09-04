"""
Client of "mqks" - Message Queue Kept Simple.
"""

### import

from critbot import crit
from functools import partial
from gevent import socket, spawn
from gevent.event import AsyncResult, Event
from gevent.queue import Queue, Empty
import logging
import random
import time
from uqid import dtid

### config

config = dict(
    workers=[                               # Should be exactly the same as mqks.server.config['workers']
        '127.0.0.1:24000:25000',
        '127.0.0.1:24001:25001',
    ],
    logger_name='mqks.client',              # Change to logger_name you configured in your service.
    ping_seconds=15,                        # Ping each connected worker each N seconds to detect disconnect.
    reconnect_seconds=1,                    # How many seconds to wait on disconnect before trying to reconnect.
    id_length=24,                           # Length of random ID. More bytes = more secure = more slow.
)

WORKERS = 0  # Updated on connect()

### state

state = {}

def init_state():
    state['socks'] = {}                     # state['socks'][worker: int] == sock: gevent._socket2.socket
    state['pingers'] = {}                   # state['pingers'][worker: int] == gevent.greenlet.Greenlet
    state['receivers'] = {}                 # state['receivers'][worker: int] == gevent.greenlet.Greenlet
    state['requests'] = {}                  # state['requests'][worker: int] = gevent.queue.Queue
    state['senders'] = {}                   # state['senders'][worker: int] == gevent.greenlet.Greenlet
    state['consumers'] = {}                 # state['consumers'][worker: int][consumer_id: str] == consumer: str
    state['workers'] = {}                   # state['workers'][consumer_id: str] == worker: int
    state['on_msg'] = {}                    # state['on_msg'][consumer_id: str] == on_msg: callable(msg: dict)
    state['on_disconnect'] = {}             # state['on_disconnect'][worker: int][consumer_id: str] == on_disconnect: callable()
    state['on_reconnect'] = {}              # state['on_reconnect'][consumer_id: str] == on_reconnect: callable(old_consumer_id: str, new_consumer_id: str)
    state['confirms'] = {}                  # state['confirms'][request_id: str] == confirm_event: gevent.event.Event
    state['eval_results'] = {}              # state['eval_results'][request_id: str] == eval_result: gevent.event.AsyncResult
    state['auto_reconnect'] = False         # bool, enabled on manual connect()

init_state()

### connect

def connect(worker=None, old_sock=None):
    """
    Connect to MQKS worker or prepare for auto-connect to multiple workers of queues and events on demand.

    @param worker: int|None
    @param old_sock: gevent._socket2.socket|None - For atomic CAS.
    """
    global WORKERS
    WORKERS = len(config['workers'])
    state['auto_reconnect'] = True

    if worker is None:
        return

    while state['socks'].get(worker) is old_sock:
        try:
            config['_log'] = logging.getLogger(config['logger_name'])
            config['_log'].info('connecting to w{}'.format(worker))

            host, _, port = config['workers'][worker].split(':')
            port = int(port)

            sock = socket.socket()
            sock.connect((host, port))

            if state['socks'].get(worker) is old_sock:  # Greenlet-atomic CAS.
                state['socks'][worker] = sock
            else:
                config['_log'].info('detected duplicate connection to w{}, closing it'.format(worker))
                sock.close()
                return

            if worker not in state['on_disconnect']:
                state['on_disconnect'][worker] = {}

            if worker not in state['pingers']:
                state['pingers'][worker] = spawn(_pinger, worker)

            if worker not in state['receivers']:
                state['receivers'][worker] = spawn(_receiver, worker)

            if worker not in state['requests']:
                state['requests'][worker] = Queue()

            if worker not in state['senders']:
                state['senders'][worker] = spawn(_sender, worker)

            if worker in state['consumers']:
                _reconsume(worker)
            else:
                state['consumers'][worker] = {}

            break

        except Exception:
            crit(also=worker)
            time.sleep(config['reconnect_seconds'])

### _on_disconnect

def _on_disconnect(worker, e, old_sock):
    """
    Handles unexpected disconnect.

    @param worker: int
    @param e: Exception
    @param old_sock: gevent._socket2.socket|None
    """
    if config['_log'].level == logging.DEBUG:
        config['_log'].debug('disconnected from w{}: {}'.format(worker, e))

    for consumer_id, on_disconnect in state['on_disconnect'][worker].items():
        try:
            on_disconnect()
        except Exception:
            crit(also=dict(worker=worker, consumer_id=consumer_id))

    time.sleep(config['reconnect_seconds'])
    if state['auto_reconnect']:
        connect(worker, old_sock=old_sock)

### _reconsume

def _reconsume(worker):
    """
    Request consume again with updated consumer_ids.
    Is called on reconnect.

    @param worker: int
    """

    consumers = state['consumers'][worker]
    for old_consumer_id, consumer in consumers.items():
        new_consumer_id = _request_id()
        consumers.pop(old_consumer_id, None)
        consumers[new_consumer_id] = consumer

        state['workers'].pop(old_consumer_id, None)
        state['workers'][new_consumer_id] = worker

        on_reconnect = state['on_reconnect'].pop(old_consumer_id, None)
        if on_reconnect:
            state['on_reconnect'][new_consumer_id] = on_reconnect
            try:
                on_reconnect(old_consumer_id, new_consumer_id)
            except Exception:
                crit(also=dict(old_consumer_id=old_consumer_id, new_consumer_id=new_consumer_id))

        on_disconnect = state['on_disconnect'][worker].pop(old_consumer_id, None)
        if on_disconnect:
            state['on_disconnect'][worker][new_consumer_id] = on_disconnect

        on_msg = state['on_msg'].pop(old_consumer_id, None)
        if on_msg:
            state['on_msg'][new_consumer_id] = on_msg

        # No need to update old "consumer_id" partial-bound into "msg.ack()" and "msg.reject()":
        # when client disconnects from server, server deletes old consumer and rejects all msgs.

        _send(worker, new_consumer_id, 'consume', consumer)

### disconnect

def disconnect():
    """
    Disconnect from all workers.
    """
    try:
        state['auto_reconnect'] = False
        for worker, sock in state['socks'].iteritems():

            for greenlet_name in 'pingers', 'receivers', 'senders':
                state[greenlet_name][worker].kill()
                del state[greenlet_name][worker]

            for on_disconnect in state['on_disconnect'][worker].values():
                try:
                    on_disconnect()
                except Exception:
                    crit()

            sock.close()
        init_state()

    except Exception:
        crit()

### publish

def publish(event, data, confirm=False):
    """
    Client publishes new message to server.
    Server puts copies of this message to zero or more queues that were subscribed to this event.

    @param event: str
    @param data: str
    @param confirm: bool
    """
    _send(get_worker(event), _request_id(), 'publish', '{} {}'.format(event, data), confirm=confirm)

### consume

def consume(queue, events, on_msg, on_disconnect=None, on_reconnect=None, delete_queue_when_unused=False, manual_ack=False, add_events=False, confirm=False):
    """
    Client starts consuming messages from queue.
    May replace subscriptions of the queue (if any) with new list of events.
    May add some events to existing subscriptions.
    When client disconnects, server deletes all consumers of this client.
    When client reconnects, it restarts all its consumers.
    If consumer with manual-ack disconnects, all not-acked messages are automatically rejected by server - returned to the queue.

    @param queue: str
    @param events: iterable(str) - Replace existing subscriptions (if any) with new "events" list. If "add_events" is set, then add "events" to existing subscriptions.
    @param on_msg: callable
    @param on_disconnect: callable
    @param on_reconnect: callable(old_consumer_id: str, new_consumer_id: str)
    @param delete_queue_when_unused: bool|float|int -
        False - Do not delete the queue.
        True - Schedule delete when queue is unused by consumers.
        5 - Schedule delete when queue is unused by consumers for 5 seconds.
    @param manual_ack: bool
    @param add_events: bool
    @param confirm: bool
    """

    consumer_id = _request_id()

    state['on_msg'][consumer_id] = on_msg

    if on_reconnect:
        state['on_reconnect'][consumer_id] = on_reconnect

    events = ' '.join(events)  # To allow any iterable.

    consumer = ''.join((
        queue,
        ' --add' if add_events else '',
        ' ' + events if events else '',
        '' if delete_queue_when_unused is False else ' --delete-queue-when-unused' + (
            '' if delete_queue_when_unused is True else '={}'.format(delete_queue_when_unused)
        ),
        ' --manual-ack' if manual_ack else '',
    ))

    state['workers'][consumer_id] = worker = get_worker(queue)
    _send(worker, consumer_id, 'consume', consumer, confirm=confirm)

    # All worker-indexed structures are created after first "_send() -> connect(worker)".
    state['consumers'][worker][consumer_id] = consumer
    if on_disconnect:
        state['on_disconnect'][worker][consumer_id] = on_disconnect

    return consumer_id

### rebind

def rebind(queue, replace=None, remove=None, add=None, remove_mask=None, confirm=False):
    """
    Replace subscriptions of the queue with new list of events, or remove some and add some other events.
    TODO: On next incompatible change, move "remove_mask" right after "remove" - to match docs and execution order at server.

    @param queue: str
    @param replace: list(str)|None - Replace subscriptions of the queue with new list of events.
    @param remove: list(str)|None - Remove some events from subscriptions of this queue.
    @param add: list(str)|None - Add some events to subscriptions of this queue.
    @param remove_mask: list(str)|None - Remove some events from subscriptions of this queue by event mask like "e1.*.a1".
    @param confirm: bool
    """

    assert replace or remove or add or remove_mask, (replace, remove, add, remove_mask)
    events = replace or []

    if remove:
        events.append('--remove')
        events.extend(remove)

    if add:
        events.append('--add')
        events.extend(add)

    if remove_mask:
        events.append('--remove-mask')
        events.extend(remove_mask)

    _send(get_worker(queue), _request_id(), 'rebind', '{} {}'.format(queue, ' '.join(events)), confirm=confirm)

### ack

def ack(consumer_id, msg_id, confirm=False):
    """
    Acknowledge this message was processed by this consumer.

    @param consumer_id: str
    @param msg_id: str
    @param confirm: bool
    """
    worker = state['workers'].get(consumer_id)
    if worker is not None:
        _send(worker, _request_id(), 'ack', '{} {}'.format(consumer_id, msg_id), confirm=confirm)

### ack all

def ack_all(consumer_id, confirm=False):
    """
    Acknowledge all messages were processed by this consumer.

    @param consumer_id: str
    @param confirm: bool
    """
    ack(consumer_id, '--all', confirm=confirm)

### reject

def reject(consumer_id, msg_id, confirm=False):
    """
    Reject this message - to return it to the queue with incremented "retry" counter.

    @param consumer_id: str
    @param msg_id: str
    @param confirm: bool
    """
    worker = state['workers'].get(consumer_id)
    if worker is not None:
        _send(worker, _request_id(), 'reject', '{} {}'.format(consumer_id, msg_id), confirm=confirm)

### reject all

def reject_all(consumer_id, confirm=False):
    """
    Reject all messages - to return them to the queue with incremented "retry" counter.

    @param consumer_id: str
    @param confirm: bool
    """
    reject(consumer_id, '--all', confirm=confirm)

### delete consumer

def delete_consumer(consumer_id, confirm=False):
    """
    Delete the consumer.
    Server will not send messages to this consumer any more.
    Client will not restart this consumer on reconnect.

    @param consumer_id: str
    @param confirm: bool
    """
    worker = state['workers'].pop(consumer_id, None)
    if worker is None:
        return

    state['consumers'][worker].pop(consumer_id, None)
    state['on_msg'].pop(consumer_id, None)
    state['on_disconnect'][worker].pop(consumer_id, None)
    state['on_reconnect'].pop(consumer_id, None)

    _send(worker, _request_id(), 'delete_consumer', consumer_id, confirm=confirm)

### delete queue

def delete_queue(queue, confirm=False):
    """
    Delete the queue instantly.
    Server will not copy messages to this queue any more.

    @param queue: str
    @param confirm: bool
    """
    _send(get_worker(queue), _request_id(), 'delete_queue', queue, confirm=confirm)

### ping

def ping(worker, data=None, confirm=False):
    """
    Ping-pong.

    @param worker: int
    @param data: str
    @param confirm: bool
    """
    _send(worker, _request_id(), 'ping', data or config['logger_name'], confirm=confirm)

### _pinger

def _pinger(worker):
    """
    Pings MQKS worker for keep-alive.

    @param worker: int
    """
    while 1:
        try:
            time.sleep(config['ping_seconds'] or 10)
            if config['ping_seconds']:
                ping(worker)
        except Exception:
            crit()

### _eval

def _eval(code, worker=0, timeout=None):
    """
    Backdoor to get any stats.
    See "stats.py"

    @param code: str - E.g. "len(state['queues'])".
    @param worker: int - From 0 to WORKERS - 1.
    @param timeout: float|None - Max seconds to wait for result. Enables "gevent.timeout.Timeout" excepton.
    @return str - Result of successful code evaluation.
    @raise Exception - Contains server-side "error_id".
    """
    eval_id = _request_id()
    state['eval_results'][eval_id] = eval_result = AsyncResult()
    _send(worker, eval_id, '_eval', code)
    return eval_result.get(timeout=timeout)

### request id

def _request_id():
    """
    Generate request id

    @return: str
    """
    return dtid(config['id_length'])

### _send

def _send(worker, request_id, action, data, confirm=False):
    """
    Send request

    @param worker: int
    @param request_id: str
    @param action: str
    @param data: str
    @param confirm: bool
    """

    if worker not in state['socks']:
        connect(worker)

    action_confirm = action + (' --confirm' if confirm else '')

    if config['_log'].level == logging.DEBUG:
        config['_log'].debug('#{} > w{}: {} {}'.format(request_id, worker, action_confirm, data))

    request = '{} {} {}\n'.format(request_id, action_confirm, data)

    if confirm:
        confirm_event = state['confirms'][request_id] = Event()
    else:
        confirm_event = None

    try:
        state['requests'][worker].put(request)

        if confirm:
            confirm_event.wait()

    finally:
        if confirm:
            state['confirms'].pop(request_id, None)

### _sender

def _sender(worker):
    """
    Send requests from queue to sock
    to avoid "This socket is already used by another greenlet" error.

    @param worker: int
    """

    try:
        requests = state['requests'][worker]
        # "requests" for worker CAN NOT be changed without kill of "_sender", so we create local name.
        # "sock" of worker CAN be changed without kill of "_sender" - on reconnect, preserving "requests".
    except Exception:
        crit()

    sock = None
    while 1:
        try:
            try:
                request = requests.peek(timeout=1)
            except Empty:
                continue

            sock = state['socks'][worker]
            try:
                sock.sendall(request)

            except Exception as e:
                _on_disconnect(worker, e, sock)
                continue

            requests.get()  # Delete request from queue.

        except Exception as e:
            crit()
            time.sleep(config['reconnect_seconds'])  # Less spam.

### _receiver

def _receiver(worker):
    """
    Receive responses.

    @param worker: int
    """
    sock = state['socks'].get(worker)
    while 1:
        try:
            sock = state['socks'][worker]
            f = sock.makefile('r')

            while 1:
                response = f.readline()
                if response == '':  # E.g. socket is broken.
                    break

                try:
                    response = response.rstrip('\r\n')  # Not trailing space in "ok " confirm.
                    request_id, response_type, data = response.split(' ', 2)

                    if config['_log'].level == logging.DEBUG:
                        config['_log'].debug('#{} < w{}: {} {}'.format(request_id, worker, response_type, data))

                    ### error

                    if response_type == 'error':
                        error = Exception(response)
                        eval_result = state['eval_results'].pop(request_id, None)
                        if eval_result:
                            eval_result.set_exception(error)
                            continue
                        raise error

                    ### confirm

                    if data == '':
                        confirm_event = state['confirms'].get(request_id)
                        if confirm_event is not None:
                            confirm_event.set()
                        continue

                    ### consume

                    on_msg = state['on_msg'].get(request_id)
                    if on_msg:
                        consumer_id = request_id

                        ### update consumer

                        if data.startswith('--update '):
                            _, consumer = data.split(' ', 1)
                            consumers = state['consumers'][worker]
                            if consumer_id in consumers:
                                consumers[consumer_id] = consumer
                            continue

                        ### msg

                        msg_id, props, data = data.split(' ', 2)

                        msg = dict(
                            id=msg_id,
                            data=data,
                            ack=partial(ack, consumer_id, msg_id),
                            reject=partial(reject, consumer_id, msg_id),
                        )

                        for prop in props.split(','):
                            name, value = prop.split('=', 1)
                            msg[name] = value

                        spawn(_safe_on_msg, on_msg, msg)
                        continue

                    ### eval_result

                    eval_result = state['eval_results'].pop(request_id, None)
                    if eval_result:
                        eval_result.set(data)

                    ### except

                except Exception:
                    crit(also=dict(worker=worker, response=response))

        except Exception as e:
            _on_disconnect(worker, e, sock)
        else:
            _on_disconnect(worker, None, sock)

### _safe_on_msg

def _safe_on_msg(on_msg, msg):
    try:
        on_msg(msg)
    except Exception:
        crit()

### get_worker

def get_worker(item):
    """
    Find out which worker serves this item, e.g. queue name.
    Items are almost uniformly distributed by workers using hash of item.

    @param item: hashable
    @return int
    """
    return hash(item) % WORKERS
