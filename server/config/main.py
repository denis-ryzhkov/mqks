
### import

from critbot import crit_defaults
import critbot.plugins.syslog
import logging
import multiprocessing as mp
import re

### config

config = dict(
    host='127.0.0.1',
    port=54321,

    ### log

    environment='PROD',
    logger_name='mqks.server',
    logger_level=logging.INFO,
    get_extra_log_plugins=lambda: [],
    grep='',  # See "stats.py".

    ### top_events

    top_events=False,
    top_events_seconds=60*5,
    top_events_limit=50,
    top_events_id=re.compile(r'[0-9a-f]{24,}|[0-9]+'),  # 24+ hex or any decimal IDs will be masked as "{id}".

    ### gbn_profile

    gbn_profile=False,  # Eval "gbn_profile.enable(),get(),disable()" from any worker to manage all workers.
    gbn_seconds=60*5,

    ### accept

    backlog=256,        # How many clients may wait for accept in shared listener.
    accepted_diff=5,    # Difference between min and max in "state.accepted_by_workers" when max stops accepting.
                        # Smaller diff increases risk of all workers not accepting until synced.
    ### other           # Bigger diff leads to bad balance.

    block_seconds=1,
    id_length=24,
    client_postfix_length=4,
    remove_mask_cache_limit=100500,     # How many compiled regexps for "--remove-mask" feature are cached before cache is cleared.
    seconds_before_gc=60,
    bytes_per_pipe=64 * 1024 - 4,
    workers=mp.cpu_count(),
)

### crit, log

def init_log():
    syslog_plugin = critbot.plugins.syslog.plugin(logger_name=config['logger_name'], logger_level=config['logger_level'])
    syslog_plugin.handler.setFormatter(logging.Formatter('.%(msecs)03d %(message)s'))
    log = logging.getLogger(config['logger_name'])

    crit_defaults.subject = '{environment} {logger_name} CRIT'.format(**config)
    crit_defaults.plugins = [syslog_plugin] + config['get_extra_log_plugins']()
    crit_defaults.crit_in_crit = log.critical
    crit_defaults.stop_spam_file.enabled = True
