from rediscluster import StrictRedisCluster

from ..utils.cluster import DockerManager

CONTAINER_PREFIX = 'redis'


class DBConnect(object):
    """Connect to all opened redis db"""
    def __init__(self):
        super(DBConnect, self).__init__()

        self.dm = DockerManager(CONTAINER_PREFIX, update_stats=False)

        ips = []
        for _, c in self.dm.containers.items():
            c.update_cache()
            ips.append(c.cache['ip'])

        startup_nodes = [{"host": ip, "port": "6379"} for ip in ips]
        self.StrictRedisCluster = StrictRedisCluster(startup_nodes=startup_nodes,decode_responses=True)
