
### import

from critbot import crit
from gbn import gbn
from gevent import spawn
from gevent.queue import Queue, Empty
import logging
import os
import time
from uqid import dtid, uqid

from mqks.server.config import config, log
from mqks.server.lib import state
from mqks.server.lib.log import verbose
from mqks.server.lib.sockets import is_disconnect
from mqks.server.lib.workers import at_request_worker

### load_actions

def load_actions():
    """
    Load actions from plugin files.
    """
    for action_file_name in os.listdir(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'actions')):
        if action_file_name == '__init__.py' or not action_file_name.endswith('.py'):
            continue

        action_name = action_file_name.replace('.py', '')
        action_module = __import__('mqks.server.actions.{}'.format(action_name), globals(), locals(), ['object'])
        state.actions[action_name] = getattr(action_module, action_name)

### on_client_connected

def on_client_connected(sock, addr):
    """
    Client connection handler.

    @param sock: gevent._socket2.socket
    @param addr: tuple(host: str, port: int)
    """
    client = request = None
    try:
        # To avoid conflicts when client reuses local port of other disconnected client - random part is added - is also good for log reading.
        client = '{1}:{2}.{0}'.format(uqid(config['client_postfix_length']), *addr)
        log.info('w{}: new client {}'.format(state.worker, client))

        state.socks_by_clients[client] = sock
        state.responses_by_clients[client] = Queue()
        spawn(responder, client)

        f = sock.makefile('r')
        while 1:
            request = dict(client=client, worker=state.worker)

            request['body'] = f.readline()
            if request['body'] == '':
                break

            assert '\t' not in request['body']  # See "spec.txt".
            on_request(request)
            time.sleep(0)

    except Exception as e:
        if not is_disconnect(e):
            crit(also=dict(client=client, request=request))

    finally:
        log.info('w{}: bye client {}'.format(state.worker, client))

        try:
            delete_consumers(client)
            state.responses_by_clients.pop(client)
            state.socks_by_clients.pop(client)

        except Exception:
            crit(also=dict(client=client))

### on_request

def on_request(request):
    """
    Handler of request from client.

    @param request: dict(
        client: str,
        worker: int,
        body: str,
    )

    "action()" gets "request" without "body" but with (
        id: str,
        action: str,
        data: str - should be parsed inside "action()",
        confirm: bool,
    )

    Command in another worker gets "request" without (
        action: str - unused,
        data: str - unused and may be very big
    )
    """
    wall = gbn('on_request')
    request['id'] = None
    try:
        body = request['body'].rstrip()
        request['id'], request['action'], request['data'] = body.split(' ', 2)
        del request['body']  # Less data to pass between workers. On error "request" with "id, action, data" will be logged.

        if log.level == logging.DEBUG or config['grep']:
            verbose('w{}: {}#{} > {} {}'.format(state.worker, request['client'], request['id'], request['action'], request['data']))

        request['confirm'] = request['data'].startswith('--confirm ')
        if request['confirm']:
            request['data'] = request['data'][10:]

        action = state.actions[request['action']]
        wall = gbn(request['action'], wall=wall)
        action(request)
        gbn(wall=wall)

    except Exception:
        request['error_id'] = dtid(config['id_length'])
        crit(also=request)
        try:
            respond(request)
        except Exception:
            crit(also=request)
        gbn(wall=wall)

### respond

@at_request_worker
def remote_respond(request, data=''):
    respond(request, data)

def respond(request, data=''):
    """
    Put response to local responses queue.
    Can not write to socket right here, as this function may be called from multiple greenlets.

    @param request: dict - defined in "on_request" + optional fields (
        error_id: str - should be set in "except"
    )
    @param data: str - response data, don't confuse it with "request['data']"
    """
    responses = state.responses_by_clients.get(request['client'])
    if responses:
        responses.put((request, data))

### responder

def responder(client):
    """
    Sends queued responses to socket of client.
    See also "mqks.server.lib.workers.respond()" that enqueues response to "state.responses_by_clients[client]".

    @param client: str
    """

    response = None
    try:
        responses = state.responses_by_clients.get(client)
        sock = state.socks_by_clients.get(client)
        if not responses or not sock:
            return

        while client in state.responses_by_clients:
            try:
                response = responses.get(timeout=config['block_seconds'])
            except Empty:
                continue

            wall = gbn('responder')
            try:
                request, data = response
                error_id = request.get('error_id')
                response = '{} {}'.format('error' if error_id else 'ok', error_id or data)
                if log.level == logging.DEBUG or config['grep']:
                    verbose('w{}: {}#{} < {}'.format(state.worker, client, request['id'], response))
                response = '{} {}\n'.format(request['id'], response)

            except Exception:
                gbn(wall=wall)
                crit(also=dict(response=response))
                continue

            try:
                sock.sendall(response)
                # Disconnect on socket error.
            finally:
                gbn(wall=wall)

            time.sleep(0)

    except Exception as e:
        if not is_disconnect(e):
            crit(also=dict(client=client, response=response))

### anti-loop import

from mqks.server.actions.delete_consumer import delete_consumers
