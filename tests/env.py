### import

import os
import subprocess
from nose.plugins import Plugin

### EnvPlugin

class EnvPlugin(Plugin):
    name = 'EnvPlugin'
    score = 1

    ### init

    def __init__(self):
        super(EnvPlugin, self).__init__()
        self.__server = None
        self.__mqks_root_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        self.__mqks_server_path = os.path.join(self.__mqks_root_path, 'server', 'mqksd')

    ### options

    def options(self, parser, env=os.environ):
        super(EnvPlugin, self).options(parser, env=env)

    ### configure

    def configure(self, options, conf):
        super(EnvPlugin, self).configure(options, conf)
        self.enabled = True

    ### begin

    def begin(self):
        self.__kill_mqks_server()
        self.__run_mqks_server()

    ### finalize

    def finalize(self, test):
        self.__kill_mqks_server()

    ### private kill mqks

    def __kill_mqks_server(self):
        if self.__server:
            self.__server.terminate()
            self.__server = None
        subprocess.call(['pkill', '-f', self.__mqks_server_path])

    ### private run mqks server

    def __run_mqks_server(self):
        self.__server = subprocess.Popen([self.__mqks_server_path], shell=True, preexec_fn=os.setsid)
