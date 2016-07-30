import sys; sys.path.append('../src')
import sys
from pprint import pprint
from distrpy.utils.cluster import DockerManager

CONTAINER_PREFIX = 'distrpy_worker_'

if __name__ == '__main__':
	dm = DockerManager(CONTAINER_PREFIX)

	port = 23000
	configs = []
	for i in range(1):
		worker_port = port+1
		host_port = port+i
		conf = {}
		conf['host_config'] = dm.client.create_host_config(
								binds={
									'/home/techteam/Distrpy/':{
										'bind': '/Distrpy',
										'mode': 'ro'
									}
								},
								port_bindings={
									worker_port: host_port
							    },
							    mem_limit='500M')
		conf['working_dir'] = '/Distrpy/example'
		conf['volumes'] = ['/Distrpy']
		conf['ports'] = [worker_port]
		configs.append(conf)

	dm.add_containers(len(configs), 'distrpy/python', 'python start_worker.py {}'.format(host_port), configs)
	for n, c in dm.containers.items():
		if n.startswith(CONTAINER_PREFIX):
			print("addr=({},{})".format(c.ip, c.ports))
	dm.shutdown()
