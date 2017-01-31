#!/usr/bin/env python

"""
Client of "mqks" - Message Queue Kept Simple.
"""

### import

from critbot import crit
from functools import partial
from gevent import killall, socket, spawn
from gevent.event import AsyncResult, Event
from gevent.queue import Queue, Empty
import logging
import time
from uqid import dtid

### config

config = dict(
    host='127.0.0.1',
    port=54321,
    logger_name='mqks.client',
    ping_seconds=15,
    reconnect_seconds=1,
    id_length=24,
)

### state

state = dict()

def init_state():
    state['consumers'] = {}
    state['on_msg'] = {}
    state['on_disconnect'] = {}
    state['on_reconnect'] = {}
    state['confirms'] = {}
    state['eval_results'] = {}
    state['requests'] = Queue()

init_state()

### connect

def connect():
    """
    Connect
    """
    while 1:
        try:
            config['_auto_reconnect'] = True

            config['_log'] = logging.getLogger(config['logger_name'])
            config['_log'].info('connecting')

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((config['host'], config['port']))
            config['_sock'] = sock

            config['_recv'] = spawn(_recv)
            _reconsume()

            if not config.get('_sender'):
                config['_sender'] = spawn(_sender)

            if config['ping_seconds'] and not config.get('_ping'):
                config['_ping'] = spawn(_ping)

            break

        except Exception:
            crit()
            time.sleep(config['reconnect_seconds'])


### _reconsume

def _reconsume():
    """
    Request consume again with updated consumer_ids.
    Is called on (re)connect.
    """

    for old_consumer_id, consumer in state['consumers'].items():
        new_consumer_id = _request_id()
        state['consumers'].pop(old_consumer_id, None)
        state['consumers'][new_consumer_id] = consumer

        on_reconnect = state['on_reconnect'].pop(old_consumer_id, None)
        if on_reconnect:
            state['on_reconnect'][new_consumer_id] = on_reconnect
            try:
                on_reconnect(old_consumer_id, new_consumer_id)
            except Exception:
                crit(also=dict(old_consumer_id=old_consumer_id, new_consumer_id=new_consumer_id))

        on_disconnect = state['on_disconnect'].pop(old_consumer_id, None)
        if on_disconnect:
            state['on_disconnect'][new_consumer_id] = on_disconnect

        on_msg = state['on_msg'].pop(old_consumer_id, None)
        if on_msg:
            state['on_msg'][new_consumer_id] = on_msg

        # No need to update old "consumer_id" partial-bound into "msg.ack()" and "msg.reject()":
        # when client disconnects from server, server deletes old consumer and rejects all msgs.

        _send(new_consumer_id, 'consume', consumer, async=False)

### disconnect

def disconnect():
    """
    Disconnect
    """
    try:
        config['_auto_reconnect'] = False

        greenlet_names = ['_recv', '_sender', '_ping']
        greenlets = [config[name] for name in greenlet_names]
        killall(greenlets)  # blocks

        for name in greenlet_names:
            config[name] = None

        config['_sock'].close()

        for consumer_id in state['consumers'].keys():
            on_disconnect = state['on_disconnect'].get(consumer_id)
            if on_disconnect:
                try:
                    on_disconnect()
                except Exception:
                    crit(also=consumer_id)

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
    _send(_request_id(), 'publish', '{} {}'.format(event, data), confirm=confirm)

### consume

def consume(queue, events, on_msg, on_disconnect=None, on_reconnect=None, delete_queue_when_unused=False, manual_ack=False, add_events=False, confirm=False):
    """
    Client starts consuming messages from queue.
    May replace subscriptions of the queue (if any) with new list of events. May add some events to existing subscriptions. Or may just use existing subscriptions.
    When client disconnects, server deletes all consumers of this client.
    When client reconnects, it restarts all its consumers.
    If consumer with manual-ack disconnects, all not-acked messages are automatically rejected by server - returned to the queue.

    @param queue: str
    @param events: iterable(str) - Replace existing subscriptions (if any) with new "events" list. Or use existing subscriptions if "events" list is empty. See also "add_events".
    @param on_msg: callable
    @param on_disconnect: callable
    @param on_reconnect: callable(old_consumer_id: str, new_consumer_id: str)
    @param delete_queue_when_unused: bool|float|int -
        False - Do not delete the queue.
        True - Schedule delete when queue is unused by consumers.
        5 - Schedule delete when queue is unused by consumers for 5 seconds.
    @param manual_ack: bool
    @param add_events: bool - Add "events" to existing subscriptions.
    @param confirm: bool
    """

    consumer_id = _request_id()

    state['on_msg'][consumer_id] = on_msg

    if on_disconnect:
        state['on_disconnect'][consumer_id] = on_disconnect

    if on_reconnect:
        state['on_reconnect'][consumer_id] = on_reconnect

    events = ' '.join(events)  # To allow any iterator.

    consumer = '{}{}{}{}{}'.format(
        queue,
        ' --add' if add_events else '',
        ' ' + events if events else '',
        '' if delete_queue_when_unused is False else ' --delete-queue-when-unused{}'.format(
            '' if delete_queue_when_unused is True else '={}'.format(delete_queue_when_unused)
        ),
        ' --manual-ack' if manual_ack else '',
    )
    state['consumers'][consumer_id] = consumer

    _send(consumer_id, 'consume', consumer, confirm=confirm)
    return consumer_id

### rebind

def rebind(queue, replace=None, remove=None, add=None, remove_mask=None, confirm=False):
    """
    Replace subscriptions of the queue with new list of events, or remove some and add some other events.

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

    _send(_request_id(), 'rebind', '{} {}'.format(queue, ' '.join(events)), confirm=confirm)

### ack

def ack(consumer_id, msg_id, confirm=False):
    """
    Acknowledge this message was processed by this consumer.

    @param consumer_id: str
    @param msg_id: str
    @param confirm: bool
    """
    _send(_request_id(), 'ack', '{} {}'.format(consumer_id, msg_id), confirm=confirm)

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
    _send(_request_id(), 'reject', '{} {}'.format(consumer_id, msg_id), confirm=confirm)

### reject all

def reject_all(consumer_id, confirm=False):
    """
    Reject all messages - to return them to the queue with incremented "retry" counter.

    @param consumer_id: str
    @param confirm: bool
    """
    reject(consumer_id, '--all', confirm=confirm)

### delete consumer from state

def delete_consumer_from_state(consumer_id):
    """
    Delete consumer from internal state
    @param consumer_id: str
    """
    state['consumers'].pop(consumer_id, None)
    state['on_msg'].pop(consumer_id, None)
    state['on_disconnect'].pop(consumer_id, None)
    state['on_reconnect'].pop(consumer_id, None)

### delete consumer

def delete_consumer(consumer_id, confirm=False):
    """
    Delete the consumer.
    Server will not send messages to this consumer any more.
    Client will not restart this consumer on reconnect.

    @param consumer_id: str
    @param confirm: bool
    """
    delete_consumer_from_state(consumer_id)
    _send(_request_id(), 'delete_consumer', consumer_id, confirm=confirm)

### delete queue

def delete_queue(queue, confirm=False):
    """
    Delete the queue instantly.
    Server will not copy messages to this queue any more.

    @param queue: str
    @param confirm: bool
    """
    _send(_request_id(), 'delete_queue', queue, confirm=confirm)

### ping

def ping(data=None, confirm=False):
    """
    Ping-pong. Used by MQKS client for keep-alive.

    @param data: str
    @param confirm: bool
    """
    _send(_request_id(), 'ping', data or config['logger_name'], confirm=confirm)

### _ping

def _ping():
    while 1:
        try:
            time.sleep(config['ping_seconds'])
            ping()
        except Exception:
            crit()

### _eval

def _eval(code, worker=None, timeout=None):
    """
    Backdoor to get any stats.
    See "stats.py"

    @param code: str - E.g. "len(state['queues'])".
    @param worker: int|None - From 0 to "config['workers'] - 1". Defaults to worker this client is connected to.
    @param timeout: float|None - Max seconds to wait for result. Enables "gevent.timeout.Timeout" excepton.
    @return str - Result of successful code evaluation.
    @raise Exception - Contains server-side "error_id".
    """
    eval_id = _request_id()
    state['eval_results'][eval_id] = result = AsyncResult()
    _send(eval_id, '_eval', code if worker is None else '--worker={} {}'.format(worker, code))
    return result.get(timeout=timeout)

### request id

def _request_id():
    """
    Generate request id

    @return: str
    """
    return dtid(config['id_length'])

### _send

def _send(request_id, action, data, confirm=False, async=True):
    """
    Send request

    @param request_id: str
    @param action: str
    @param data: str
    @param confirm: bool
    @param async: bool
    """

    action_confirm = action + (' --confirm' if confirm else '')

    if config['_log'].level == logging.DEBUG:
        config['_log'].debug('#{} > {} {}'.format(request_id, action_confirm, data))

    request = '{} {} {}\n'.format(request_id, action_confirm, data)

    _confirm = None
    if confirm:
        _confirm = state['confirms'][request_id] = Event()

    try:
        if async:
            state['requests'].put(request)
        else:
            config['_sock'].sendall(request)

        if confirm and _confirm:
            _confirm.wait()

    finally:
        if confirm:
            state['confirms'].pop(request_id, None)

### _sender

def _sender():
    """
    Send requests from queue to sock
    to avoid "This socket is already used by another greenlet" error.
    """

    while 1:
        try:
            try:
                request = state['requests'].peek(timeout=1)
            except Empty:
                continue

            try:
                config['_sock'].sendall(request)
            except Exception:
                time.sleep(config['reconnect_seconds'])  # Give other clients time to reconnect and start consuming.
                raise

            state['requests'].get()  # Delete request from queue.

        except Exception as e:
            if not is_disconnect(e):
                crit()

### _recv

def _recv():
    """
    Receive responses
    """
    try:
        sock = config['_sock']

        f = sock.makefile('r')
        while 1:
            # 1/0  # Test for "on_disconnect".
            response = f.readline()
            if response == '':  # E.g. socket is broken.
                break

            try:
                response = response.rstrip('\r\n')  # Not trailing space in "ok " confirm.
                request_id, response_type, data = response.split(' ', 2)

                if config['_log'].level == logging.DEBUG:
                    config['_log'].debug('#{} < {} {}'.format(request_id, response_type, data))

                if response_type == 'error':
                    error = Exception(response)
                    eval_result = state['eval_results'].pop(request_id, None)
                    if eval_result:
                        eval_result.set_exception(error)
                        continue
                    raise error

                if data == '':  # "ok " confirm.
                    _confirm = state['confirms'].get(request_id)
                    if _confirm is not None:
                        _confirm.set()
                    continue

                on_msg = state['on_msg'].get(request_id)
                if on_msg:
                    msg_id, props, data = data.split(' ', 2)

                    msg = dict(
                        id=msg_id,
                        data=data,
                        ack=partial(ack, request_id, msg_id),
                        reject=partial(reject, request_id, msg_id),
                    )

                    for prop in props.split(','):
                        name, value = prop.split('=', 1)
                        msg[name] = value

                    spawn(_safe_on_msg, on_msg, msg)

                else:
                    eval_result = state['eval_results'].pop(request_id, None)
                    if eval_result:
                        eval_result.set(data)

            except Exception:
                crit(also=response)

    except Exception as e:
        if not is_disconnect(e):
            crit()

    finally:

        for consumer_id, on_disconnect in state['on_disconnect'].items():
            try:
                on_disconnect()
            except Exception:
                crit(also=consumer_id)

        time.sleep(config['reconnect_seconds'])
        if config['_auto_reconnect']:
            connect()

### _safe_on_msg

def _safe_on_msg(on_msg, msg):
    try:
        on_msg(msg)
    except Exception:
        crit()

### is_disconnect

def is_disconnect(e):
    estr = repr(e)
    return 'Connection reset by peer' in estr or 'Broken pipe' in estr or 'Bad file descriptor' in estr

### tests

def _tests():
    # See usage in "load_test.sh".

    ### import

    import gevent.monkey
    gevent.monkey.patch_all()

    import sys

    import critbot.plugins.syslog
    from critbot import crit_defaults
    crit_defaults.plugins = [critbot.plugins.syslog.plugin(logger_name=config['logger_name'], logger_level=logging.INFO)]

    ### args

    config['host'], client_index, messages = sys.argv[1:] if len(sys.argv) > 1 else ('localhost', 1, 30000)
    queue = 'q{}'.format(client_index)
    event = 'e{}'.format(client_index)
    n = int(messages)

    ### init

    connect()

    state = dict(consumed=0)
    done = Event()

    def publisher():
        for data in xrange(n, 0, -1):
            publish(event, data)  # confirm=True
            # time.sleep(0)

    def on_msg(msg):
        # config['_log'].info('got {}'.format(msg))
        # msg.ack()

        state['consumed'] += 1
        if state['consumed'] == n:
            done.set()

    def on_disconnect():
        config['_log'].info('on_disconnect!')

    def on_reconnect(old_consumer_id, new_consumer_id):
        config['_log'].info('on_reconnect!')
        state['consumer_id'] = new_consumer_id

    state['consumer_id'] = consume(queue, [event], on_msg, on_disconnect=on_disconnect, on_reconnect=on_reconnect, delete_queue_when_unused=5, confirm=True)  # manual_ack=True, add_events=True

    ### run test

    start = time.time()
    spawn(publisher)
    done.wait()
    print(time.time() - start)

    ### finish

    delete_consumer(state['consumer_id'])

if __name__ == '__main__':
    _tests()
