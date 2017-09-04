
### import

import logging

from mqks.server.config import config, log

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
