from mqks.server.config.main import *

config['host'] = '127.0.0.1'

if isinstance(config['workers'], list):  # back-compat
    # from mqks.server.config.mqks_workers import mqks_workers
    # config['workers'] = mqks_workers
    config['workers'] = [
        '127.0.0.1:24000:25000',
        '127.0.0.1:24001:25001',
        '127.0.0.1:24002:25002',  # 3 workers make worker of q1 != worker of e1 (used in tests)
    ]

# config['environment'] = 'TEST'
# config['logger_level'] = logging.DEBUG

# import critbot.plugins.slack
# config['get_extra_log_plugins'] = lambda: [critbot.plugins.slack.plugin(token='SECRET', channel='#avengers', users='@denisr @parf')]
