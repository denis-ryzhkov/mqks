"""
Simple client

@author Dmitry Parfyonov <dp@bduist.ru>
"""

### import

from collections import defaultdict
from gevent import spawn, kill, socket, sleep, with_timeout
from uqid import uqid

### constants

REQUEST_ID_LENGTH = 24

### SimpleClient

class SimpleClient(object):

    ### init

    def __init__(self, host, port):
        """
        Init
        @param host: str
        @param port: int
        """
        self.__host = host
        self.__port = port
        self.__sock = None
        self.__responses = defaultdict(list)
        self.__recv_gl = None

    ### wait server

    def wait_server(self):
        """
        Wait server
        """
        while 1:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn = sock.connect_ex((self.__host, self.__port))
            if conn == 0:
                sock.close()
                break
            sock.close()
            sleep(0.1)

    ### connect

    def connect(self):
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.connect((self.__host, self.__port))
        self.__recv_gl = spawn(self.__recv)

    ### send

    def send(self, command):
        """
        Send command
        @param command: str
        @return: str - request_id
        """
        request_id = uqid(REQUEST_ID_LENGTH)
        self.__sock.send('{} {}\n'.format(request_id, command))

        return request_id

    ### get response

    def get_response(self, request_id, timeout=None):
        """
        Get response
        @param request_id: str
        @param timeout: int or None - wait response in seconds
        @return: str or None
        """

        def wait_response(rid):
            while 1:
                try:
                    r = self.__responses[rid].pop(0)
                    return r
                except IndexError:
                    pass

                sleep(0.01)

        if timeout:
            return with_timeout(timeout, wait_response, request_id, timeout_value=None)

        return wait_response(request_id)

    ### close

    def close(self):
        """
        Close connection
        """
        if self.__recv_gl:
            kill(self.__recv_gl)
        if self.__sock:
            self.__sock.close()

    ### private recv

    def __recv(self):
        """
        Receive messages from server
        """
        fsock = self.__sock.makefile('r')
        while 1:
            response = fsock.readline()
            if response == '':
                break

            response_request_id, response = response.rstrip().split(' ', 1)
            self.__responses[response_request_id].append(response)

            sleep(0.01)
