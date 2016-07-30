from docker import Client
from docker.utils import kwargs_from_env
from platform import platform
from threading import Event
from pprint import pprint
import docker
import json

from .logger import Logger
from .tools import glob_executor

log = Logger(__name__)
log.setLevel(1)

CONTAINER_IMAGE = 'distrpy/worker'
CONTAINER_CMD = 'python3 start_worker.py'


class DockerManager():
    """ Tool for container management.
        Connection to docker daemon.
        Shares docker resources among distrpy classes.

        [Usage]
        # will find all containers' name starts with 'test_'
        >>> dm = DockerManager('test_')
        >>> dm.containers
        # assume one container already exists
        {'test_0': <distrpy.utils.cluster.Container object at 0x1072aeda0>}
        >>> dm.add_containers(3, image='ubuntu', cmd='python start_worker.py')
        =INFO= (distrpy.utils.cluster) : Created test_1 : d3085fafb36a7334f3e5fc8f5af9892025174e5358...
        =INFO= (distrpy.utils.cluster) : Created test_2 : af8c96d1f74fa864ad988625a27d3c1d0b5cfa0118...
        =INFO= (distrpy.utils.cluster) : Created test_3 : 21b1e36c3d5a1e4e41cda9695dcced513fba2de14b...
        >>> dm.stop_all()
        >>> dm.rm_all()

        ! IMPORTANT !: Do not share any docker resources
                    (any docker-related objects) to workers
                    as they are executed inside containers.
    """
    def __init__(self, container_prefix=None, update_stats=True):
        super(DockerManager, self).__init__()
        if platform().startswith('Darwin'): # OSX
            self.client = Client(**kwargs_from_env(assert_hostname=False)) # for Mac
        elif platform().startswith('Linux'):
            self.client = Client(base_url='unix://var/run/docker.sock') # for Linux

        if not isinstance(container_prefix, str):
            raise TypeError("'container_prefix' type should be `str`")
        self.prefix = container_prefix
        self.containerID = 0 # ID for new created containers,
                             # for naming purpose,
                             # must not conflict with existing ones.
        self.containers = {}
        self.update_containers(update_stats)

    def stop_all(self):
        """ Stop all distrpy containers. """
        for _,c in self.containers.items():
            log.info("stop {} ".format(c.id))
            self.client.stop(c.id)

    def rm_all(self):
        """ Remove all distrpy containers. """
        for _,c in self.containers.items():
            log.info("rm {} ".format(c.id))
            self.client.remove_container(c.id)

    def update_containers(self, update_stats=True):
        """ Updates `self.containers`.
            Call this method on initialization and
            whenever a container for distrpy is created or closed.
            Also updates `self.containerID`.
        """
        unchecked = list( self.containers.keys() )
        for c in self.client.containers(all=True):
            name = c['Names'][0][1:]
            if 'Created' in c['Status'] or 'Exited' in c['Status']:
                # if a existing container is not started,
                # them remove it.
                self.client.remove_container(c['Id'])
                print('removed container {}'.format(name))
                continue
            if name.startswith(self.prefix):
                if name in unchecked:
                    self.containers[name].update_cache()
                    unchecked.remove(name)
                else:
                    # add a new container to `self.containers`
                    self.containers[name] = Container(self.client, c['Id'], update_stats)
        # remove containers that are not existed anymore
        for name in unchecked:
            c = self.containers.pop(name)
            del c # release Container
        # update `self.containerID`
        for name in self.containers.keys():
            n = name.split(self.prefix)[1]
            n = int(n) if n != '' else -1 # in case there is container has no ID
            if n >= self.containerID:
                self.containerID = n + 1

    def add_containers(self, n, image, cmd, configs=None, *, update_stats=True):
        """ Create and start n containers.

            [Params]
            n:      int, number of containers to create
            image:  str, docker image for containers
            cmd:    str, command to execute on start
            configs: list of dict, configurations for containers,
                    number of configs must be the same as `n`.

                    Currently supports:

                    - host_config: created by `docker.Client.create_host_config`
                    Details: http://docker-py.readthedocs.io/en/stable/hostconfig/

                    - volumes: `volumes` argument for `create_container`.
                    Details: http://docker-py.readthedocs.io/en/stable/volumes

                    - ports: `ports` argument for `create_container`.
                    Details: http://docker-py.readthedocs.io/en/stable/port-bindings/

                    - working_dir: `working_dir` argument for `create_container`.
                    Details: http://docker-py.readthedocs.io/en/stable/api/#create_container

            NOTE: To expose ports to host, configs['ports'] must be the same as `container_port` in
                docker.Client.create_host_configs(port_bindings={``container_port`` : host_port})

        """
        if not isinstance(n, int):
            raise TypeError("`n` should be type of int")
        if not isinstance(image, str):
            raise TypeError("`image` should be type of str")
        if not isinstance(cmd, str):
            raise TypeError("`cmd` should be type of str")
        if configs and (not isinstance(configs, list) or not isinstance(configs[0], dict)):
            raise TypeError("`configs` should be type of list and elements of `configs` should be dict")
        if configs and len(configs) != n:
            raise TypeError("length of `configs` is not the same as the value of `n`")

        for i in range(n):
            name = self.prefix + str(self.containerID)
            self.containerID += 1

            if configs:
                host_config = configs[i]['host_config'] if 'host_config' in configs[i] else None
                volumes = configs[i]['volumes'] if 'volumes' in configs[i] else None
                ports = configs[i]['ports'] if 'ports' in configs[i] else None
                working_dir = configs[i]['working_dir'] if 'working_dir' in configs[i] else None
            else:
                host_config = volumes = ports = working_dir = None

            while True:
                print("host_config")
                pprint(host_config)
                try:
                    c = self.client.create_container( name=name,
                                                      image=image,
                                                      command=cmd,
                                                      host_config=host_config,
                                                      volumes=volumes,
                                                      ports=ports,
                                                      working_dir=working_dir,
                                                      detach=False )
                    self.client.start(c['Id'])
                except docker.errors.APIError as e:
                    log.debug("docker.errors.APIError:", e)
                    if 'The name' in str(e):
                        rmed = False
                        for n, cc in self.client.containers(all=True):
                            if n == name and cc['Status'] == 'Created':
                                self.client.remove_container(cc['Id'])
                                rmed = True
                        if not rmed:
                            raise RuntimeError("Container name conflicted but cannot be removed.")
                    elif 'port is already allocated' in str(e):
                        self.client.remove_container(c['Id'], force=True)
                        # change ports exported on host to avoid conflicts
                        # resolve_conflict_ports(ports, host_config, self.client.containers())
                        # continue
                        log.error("Port is already in use.")
                        break
                    elif 'Cannot start container' in str(e):
                        log.error("Cannot start container {}:{}".format(name, c['Id']),
                                  ", maybe `working_dir` doesn't exist.")
                        self.client.remove_container(c['Id'])
                        break
                    else:
                        log.error("Unhandled docker.errors.APIError execption:", e)
                        self.client.remove_container(c['Id'])
                        break
                else:
                    log.info("Started container {} : {}".format(name, c['Id']))
                    self.containers[name] = Container(self.client, c['Id'], update_stats)
                    break

    def shutdown(self):
        for n, c in self.containers.items():
            c.close_stats_stream.set()

# ========================================================================= #

# ========================================================================= #

class Container():
    """ A class for storing a container's info. """
    def __init__(self, client, Id, update_stats=True):
        super(Container, self).__init__()
        self.client = client
        self.id = Id
        self.cache = {} # cache rarely changed info because `docker inspect` is expensive
        self.update_cache()
        self.stats = None
        self.close_stats_stream = Event()
        if update_stats:
            glob_executor.submit(self.update_stats) # start update stats loop

    def info(self):
        """ Get container info with `docker inspect`.
            # TODO: make docker inspect asyncronous
        """
        return self.client.inspect_container(self.id)

    def update_cache(self):
        """ Update `self.cache`.

            ! IMPORTANT !: Only cache rarely changed info, eg. IP
                        Don't cache frequently changed ones, eg. status
        """
        info = self.info()
        self.id = info['Id'] # update to full length
        self.cache['name'] = info['Name'][1:]
        self.cache['ip'] = info['NetworkSettings']['IPAddress']
        # ports format : {'15001/tcp': [{'HostIp': '0.0.0.0','HostPort': '15000'}]}
        self.cache['ports'] = info['NetworkSettings']['Ports']
        self.cache['image'] = info['Config']['Image']
        self.cache['cpu'] = 0.0 # updated every sec

    def get_status(self):
        return self.info()['State']['Status']

    def calc_cpu_usage(self, prevtotal, prevsys):
        """ Return CPU uasge in percentage. """
        percpu = self.stats['cpu_stats']['cpu_usage']['percpu_usage'] # a list
        totalcpu = self.stats['cpu_stats']['cpu_usage']['total_usage']
        syscpu = self.stats['cpu_stats']['system_cpu_usage']
        cpuPercent = 0.0
        # calculate the change for the cpu usage of the container in between readings
        cpuDelta = totalcpu - prevtotal
        # calculate the change for the entire system between readings
        systemDelta = syscpu - prevsys
        if systemDelta > 0.0 and cpuDelta > 0.0:
            cpuPercent = (cpuDelta / systemDelta) * len(percpu) * 100.0
        return cpuPercent

    def update_stats(self):
        stream = self.client.stats(container=self.cache['name'], stream=True)
        if self.close_stats_stream.is_set():
            return
        for s in stream:
            if self.close_stats_stream.is_set():
                return
            self.stats = json.loads(s.decode("utf-8"))

            # calculate cpu usage in percentage
            prevtotal = self.stats['precpu_stats']['cpu_usage']['total_usage']
            prevsys = self.stats['precpu_stats']['system_cpu_usage']
            self.cache['cpu'] = self.calc_cpu_usage(prevtotal, prevsys)

    @property
    def name(self):
        return self.cache['name']

    @property
    def ip(self):
        return self.cache['ip']

    @property
    def ports(self):
        return self.cache['ports']

    @property
    def image(self):
        return self.cache['image']

    @property
    def cpu(self):
        return self.cache['cpu']

    @property
    def memory(self):
        return self.stats['memory_stats']['usage']  # covert to MB

    @property
    def max_memory(self):
        return self.stats['memory_stats']['max_usage']

    @property
    def memory_limit(self):
        return self.stats['memory_stats']['limit']

    @property
    def status(self):
        return self.stats


def resolve_conflict_ports(ports, host_config, containers):
    used_ports = get_used_ports(containers)
    print("used:", used_ports)
    for i, p in enumerate(ports):
        if p in used_ports:
            old = p
            while True:
                if ports[i] not in used_ports:
                    break
                ports[i] += 1 # update `ports`
            print("update port to:", ports[i])
            # update the port in `host_config`
            update_host_config_port(host_config, old, ports[i])

def get_used_ports(containers):
    used = []
    for c in containers:
        print("container::", c)
        for p in c['Ports']:
            if 'PrivatePort' in p:
                used.append(int(p['PrivatePort']))
    return used

def update_host_config_port(host_config, old, new):
    import re
    to_del_key = []
    if 'PortBindings' in host_config:
        tmp_host_config = dict(host_config['PortBindings'])
        for k, pp in tmp_host_config.items():
            if str(old) in k:
                head = re.findall(r'\d+', k)[0]
                tail = k.split(head)[1]
                head = str(new)
                host_config['PortBindings'][head+tail] = host_config['PortBindings'].pop(k)
                break

# def get_host_port(container_ports, name):
#     # assume only one port opened in host
#     for k, pp in container_ports.items():
#         for p in pp:
#             if 'HostPort' in p:
#                 return p['HostPort']
#     return None