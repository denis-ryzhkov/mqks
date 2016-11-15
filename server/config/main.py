
### import

from adict import adict
from critbot import crit_defaults
import critbot.plugins.syslog
import logging

### config

config = adict(
    host='127.0.0.1',
    port=54321,

    environment='PROD',
    logger_name='mqks.server',
    logger_level=logging.INFO,

    block_seconds=1,
    multi_consume_sleep_seconds=0.00001,  # Limits to 100K messages per second, but allows to load-balance 4/4 workers instead of 3/4.
    id_length=24,
    seconds_before_gc=60,
)

### crit, log

def init_log():
    crit_defaults.subject = '{environment} {logger_name} CRIT'.format(**config)
    crit_defaults.plugins.insert(0, critbot.plugins.syslog.plugin(logger_name=config.logger_name, logger_level=config.logger_level))
    log = logging.getLogger(config.logger_name)
    crit_defaults.crit_in_crit = log.critical
    crit_defaults.stop_spam_file.enabled = True
    return log
