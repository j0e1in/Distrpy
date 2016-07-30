from docker import Client


from ..utils.redistrib import command
from ..utils.cluster import DockerManager
# from distrpy.server.dbmaster import DBMaster
# dm = DBMaster(1,1)

CONTAINER_PREFIX = 'redis'


class DBMaster():
    """ In charge of database container management and distributed data accessing/storing. """
    def __init__(self, masterNum, replicate):
        super(DBMaster, self).__init__()

        self.port = 6379
        self.master_slave = {}
        masters = []
        slaves = []

        self.dm = DockerManager(CONTAINER_PREFIX)
        configs = []
        for i in range(masterNum + masterNum * replicate):
            conf = {}
            conf['host_config'] = self.dm.client.create_host_config(port_bindings={self.port: None}, mem_limit='10M')
            conf['ports'] = [self.port]
            configs.append(conf)

        self.dm.add_containers(len(configs), 'redis', 'redis-server --appendonly yes --cluster-enabled yes --cluster-node-timeout 5000', configs)

        for _, c in self.dm.containers.items():
            c.update_cache()
            if len(masters) < masterNum:
                masters.append(c.cache['ip'])
            else:
                slaves.append(c.cache['ip'])

        self.start_cluster([(m, self.port) for m in masters])
        print('start')
        slaveN = 0
        for r in range(replicate):
            for m in masters:
                s = slaves[slaveN]
                if m in self.master_slave:
                    self.master_slave.append[m].appned(s)
                else:
                    self.master_slave[m] = s
                self.add_slave(m, self.port, s, self.port)
                slaveN += 1

    def start_cluster(self, startup_nodes):
        print(startup_nodes)
        command.start_cluster_on_multi(startup_nodes, max_slots=16384)

    def add_master(self, nodeIP, nodePort, newIP, newPort):
        command.join_cluster(nodeIP, nodePort, newIP, newPort)

    def add_slave(self, masterIP, masterPort, newIP, newPort):
        command.replicate(masterIP, masterPort, newIP, newPort)

    def quit_cluster(self, nodeIP, nodePort):
        command.quit_cluster(nodeIP, nodePort)
