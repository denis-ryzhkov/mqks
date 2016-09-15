
### import

from critbot import crit_defaults
import critbot.plugins.syslog
import logging

### main

host = '127.0.0.1'
port = 54321

logger_name = 'mqks.server'
logger_level = logging.INFO

### fine-tuning

block_seconds = 1
id_length = 24
seconds_before_gc = 60

### crit, log

crit_defaults.plugins = [critbot.plugins.syslog.plugin(logger_name=logger_name, logger_level=logger_level)]
log = logging.getLogger(logger_name)

# lgh = logging.StreamHandler()
# log.addHandler(lgh)
