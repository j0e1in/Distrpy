
import sys; sys.path.append('../src')
import argparse

from distrpy.utils.redistrib import command
from distrpy.utils.cluster import DockerManager

CONTAINER_PREFIX = 'redis'


def create():
    dm = DockerManager(CONTAINER_PREFIX, update_stats=False)
    port = 6379
    masterNum = 2
    replicate = 1

    configs = []
    for i in range(masterNum + masterNum * replicate):
        conf = {}
        conf['host_config'] = dm.client.create_host_config(port_bindings={port: None})
        conf['ports'] = [port]
        configs.append(conf)

    dm.add_containers(len(configs), 'redis', 'redis-server --appendonly yes --cluster-enabled yes --cluster-node-timeout 5000', configs, update_stats=False)

    masters = []
    slaves = []
    for _, c in dm.containers.items():
            c.update_cache()
            if len(masters) < masterNum:
                masters.append(c.cache['ip'])
            else:
                slaves.append(c.cache['ip'])

    startup_nodes = [(m, port) for m in masters]
    print('Masters')
    print('\n'.join(masters))
    command.start_cluster_on_multi(startup_nodes, max_slots=16384)
    print('Start cluster')

    slaveN = 0
    for r in range(replicate):
        for m in masters:
            s = slaves[slaveN]
            command.join_cluster(m, port, s, port)
            print('Add slave: {}'.format(s))
            slaveN += 1


def stop():
    dm = DockerManager(CONTAINER_PREFIX, update_stats=False)
    dm.stop_all()


def remove():
    dm = DockerManager(CONTAINER_PREFIX, update_stats=False)
    dm.rm_all()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('action', choices=['create', 'stop', 'remove'])
    args = parser.parse_args()

    if args.action == 'create':
        create()
    elif args.action == 'stop':
        stop()
    elif args.action == 'remove':
        remove()
