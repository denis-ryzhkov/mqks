
### import

from adict import adict
from critbot import crit_defaults
import critbot.plugins.syslog
import logging
import multiprocessing as mp

### config

config = adict(
    host='127.0.0.1',
    port=54321,

    environment='PROD',
    logger_name='mqks.server',
    logger_level=logging.INFO,
    get_extra_log_plugins=lambda: [],
    grep='',  # See "stats.py".
    enable_gprofiler=False,
    enable_gbn=False,  # See "profile" lib.

    backlog=256,        # How many clients may wait for accept in shared listener.
    accepted_diff=5,    # Difference between min and max in "state.accepted_by_workers" when max stops accepting.
                        # Smaller diff increases risk of all workers not accepting until synced.
                        # Bigger diff leads to bad balance.

    block_seconds=1,
    id_length=24,
    client_postfix_length=4,
    seconds_before_gc=60,
    seconds_before_profile=15,
    bytes_per_pipe=64 * 1024 - 4,
    workers=mp.cpu_count(),
)

### crit, log

def init_log():
    syslog_plugin = critbot.plugins.syslog.plugin(logger_name=config.logger_name, logger_level=config.logger_level)
    syslog_plugin.handler.setFormatter(logging.Formatter('.%(msecs)03d %(message)s'))
    log = logging.getLogger(config.logger_name)

    crit_defaults.subject = '{environment} {logger_name} CRIT'.format(**config)
    crit_defaults.plugins = [syslog_plugin] + config.get_extra_log_plugins()
    crit_defaults.crit_in_crit = log.critical
    crit_defaults.stop_spam_file.enabled = True


