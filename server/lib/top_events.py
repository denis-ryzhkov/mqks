
### import

from critbot import crit
from gbn import gbn
import time

from mqks.server.config import config, log
from mqks.server.lib import state

### top_events_log_and_reset

def top_events_log_and_reset():
    """
    Reports top events from time to time, if enabled.
    """
    while 1:
        try:
            time.sleep(config['top_events_seconds'])

            if config['top_events']:
                wall = gbn('top_events')
                rows = sorted(state.top_events.iteritems(), key=lambda row: -row[1])[:config['top_events_limit']]  # Sort by "published" desc.
                state.top_events.clear()  # Before context switch on logging IO.
                log.info('w{}: top events: {}'.format(state.worker, ' '.join('{}={}'.format(*row) for row in rows)))
                gbn(wall=wall)

        except Exception:
            crit()
