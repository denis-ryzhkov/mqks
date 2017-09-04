#!/usr/bin/env python

"""
Stats of "mqks" - Message Queue Kept Simple.

Lib/script that detects number of workers,
queries all stats from each worker,
returns/prints aggregated result.
"""

### import

if __name__ == '__main__':

    import gevent.monkey
    gevent.monkey.patch_all()

    import os, sys
    sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from ast import literal_eval
from collections import defaultdict
from gevent import joinall, spawn
from mqks.client import mqks

### spells

spells = [

    # How many messages were published:
    ('messages_published', 'state.published'),

    # How many copies of messages were put to queues:
    ('messages_queued', 'state.queued'),

    # How many messages were consumed from queues:
    ('messages_consumed', 'state.consumed'),

    # How many queues are served by this worker:
    ('queues', 'len(state.queues)'),

    # How many queues are used by consumers now:
    ('queues_used_by_consumers', 'len([u for u in state.queues_used.itervalues() if u.is_set()])'),

    # How many queues will be deleted when unused by consumers:
    ('queues_to_delete_when_unused', 'len(state.queues_to_delete_when_unused)'),

    # How many commands were put to other workers:
    ('commands_put', 'state.commands_put'),

    # How many commands were got from other workers:
    ('commands_got', 'state.commands_got'),

    # How many commands are waiting to be sent to other workers:
    ('commands_to_workers', 'sum(c.qsize() for c in state.commands_to_workers.itervalues())'),

    # How many messages are stored in queues now:
    ('messages_in_queues', 'sum(q.qsize() for q in state.queues.itervalues())'),

    # Top queues by not consumed messages:
    # ./mqks_eval '--worker=13 sorted(((q.qsize(), n) for n, q in state.queues.iteritems()), key=lambda x: -x[0])[:10]'

    # How many consumed messages are waiting for ack or reject:
    ('messages_waiting_ack', 'sum(len(ms) for ms in state.messages_by_consumer_ids.itervalues())'),

    # Top queues by not acked/rejected messages:
    # ./mqks_eval '--worker=13 [(len(msgs), state.queues_by_consumer_ids[c]) for c, msgs in sorted(state.messages_by_consumer_ids.iteritems(), key=lambda c_msgs: -len(c_msgs[1]))[:10]]'

    # Content of not acked/rejected messages:
    # ./mqks_eval '--worker=13 "  --  ".join(sorted(__import__("itertools").chain(*(msgs.itervalues() for msgs in state.messages_by_consumer_ids.itervalues() if msgs)))[:100])' | perl -pe 's/  --  /\n/g'

    # Delete messages containing "victim" from not acked/rejected messages:
    # ./mqks_eval '--worker=13 len([msgs.pop(id, None) for msgs in state.messages_by_consumer_ids.values() for id, msg in msgs.items() if "victim" in msg])'

    # How many responses are waiting to be sent to client:
    ('responses_waiting', 'sum(q.qsize() for q in state.responses_by_clients.itervalues())'),

    # How many events are queues subscribed to:
    ('events_subscribed', 'len(state.queues_by_events)'),

    # How many queues are subscribed to events:
    ('queues_subscribed', 'len(state.events_by_queues)'),

    # How many consumers exist now:
    ('consumers', 'len(state.queues_by_consumer_ids)'),

    # How many consumers exists now (mirror relation):
    ('consumers_by_queues', 'sum(len(cs) for cs in state.consumers_by_queues.itervalues())'),

    # How many clients of consumers are known to this worker, should be not greater than total number of clients:
    ('clients_of_consumers_known', 'len(state.consumer_ids_by_clients)'),

    # How many clients of consumers are known (mirror relation):
    ('clients_by_consumers', 'len(set(state.clients_by_consumer_ids.itervalues()))'),

    # How many clients are connected:
    ('clients_connected', 'len(state.socks_by_clients)'),

    # Change logging level:
    # for N in {0..63}; do ./mqks_eval "--worker=$N log.setLevel(logging.DEBUG)"; done

    # Log requests and responses with this substring in INFO mode to avoid too spammy and slow full DEBUG:
    # for N in {0..63}; do ./mqks_eval "--worker=$N config.update(grep='some substring')"; done
    # for N in {0..63}; do ./mqks_eval "--worker=$N config.update(grep='')"; done  # Disable.
]

### stats

def stats(spell_names=None, timeout=None):
    """
    Get list of stats: ordered by spell, then by worker.

    @param spell_names: list(str)|None - Return only these stats. All by default.
    @param timeout: float|None - Max seconds to wait for result of each of N + 1 "_eval".
    @return list(list(
        spell_name: str,
        list(worker_result: int),
    ))
    """
    workers = int(mqks._eval("WORKERS", timeout=timeout))
    target_spells = [(spell_name, spell) for spell_name, spell in spells if spell_name in spell_names] if spell_names else spells

    # It is cheaper to route combined spell to each worker.
    # Also, this makes results from the same worker better linked to each other.
    combined_spell = '[{}]'.format(', '.join(spell for _, spell in target_spells))

    greenlets = [spawn(mqks._eval, combined_spell, worker=worker, timeout=timeout) for worker in xrange(workers)]
    joinall(greenlets)
    results = [literal_eval(greenlet.value) for greenlet in greenlets]
    # results[worker][spell_index] == result

    return [
        [spell_name, [int(results[worker][spell_index]) for worker in xrange(workers)]]
        for spell_index, (spell_name, _) in enumerate(target_spells)
    ]

### main

def main():

    debug = False
    if debug:
        from critbot import crit_defaults
        import critbot.plugins.syslog
        import logging
        crit_defaults.plugins = [critbot.plugins.syslog.plugin(logger_name=mqks.config['logger_name'], logger_level=logging.DEBUG)]

    from mqks.server.config import config as server_config
    mqks.config['workers'] = server_config['workers']
    mqks.connect()

    result = stats()

    if '--json' in sys.argv:
        print(result)
        return

    # split by hosts
    hosts = [host for host, _, _ in (worker.split(':') for worker in mqks._eval("' '.join(config['workers'])").split(' '))]
    # hosts = ['127.0.0.1', '127.0.0.1', '10.0.0.1', '127.0.0.1']  # Test.

    unique_hosts = []  # Avoid "set()" here to preserve order of hosts as in config.
    for host in hosts:
        if host not in unique_hosts:
            unique_hosts.append(host)

    for host in unique_hosts:
        print(host)

        host_workers = [worker for worker in xrange(len(hosts)) if hosts[worker] == host]

        host_result = [
            [spell_name, [worker_results[worker] for worker in host_workers]]
            for spell_name, worker_results in result
        ]

        # find max len in columns and expand worker_results
        columns_max_len = defaultdict(int)
        result_out = []
        for spell_name, worker_results in host_result:
            sum_results = sum(worker_results)
            worker_results.append(spell_name)
            worker_results.append(sum_results)

            for k, v in enumerate(worker_results):
                value_len = len(str(v))
                if value_len > columns_max_len[k]:
                    columns_max_len[k] = value_len

            result_out.append(worker_results)

        # multiplication
        published_row, queued_row = result_out[:2]
        assert published_row[-2] == 'messages_published'
        assert queued_row[-2] == 'messages_queued'
        multiplication_row = [
            'multiplication' if isinstance(p, str) else
            0 if not p else
            q / p for p, q in zip(published_row, queued_row)
            # No "float" intentionally, as it can overflow, unlike long int div.
        ]
        result_out.insert(0, multiplication_row)

        # make template
        markers = ['{{:>{}}}'.format(l) for l in columns_max_len.values()]
        titles_column_num = len(columns_max_len) - 2
        markers[titles_column_num] = markers[titles_column_num].replace('>', '<')
        template = ' '.join(markers)

        header = template.format(*([str(x) for x in host_workers] + ['workers', str(titles_column_num)]))
        print(header)
        for worker_results in result_out:
            print(template.format(*[str(x) for x in worker_results]))
        print(header)
        print('')

if __name__ == '__main__':
    main()
