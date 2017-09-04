# noinspection PyUnresolvedReferences
from mqks.server.config.main import *

# noinspection PyUnresolvedReferences
from mqks.server.config.local import *

WORKERS = len(config['workers'])
log = logging.getLogger(config['logger_name'])
