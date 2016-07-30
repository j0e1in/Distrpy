import sys; sys.path.append('../src')
import sys

from distrpy.worker.worker import Worker

if __name__ == '__main__':
	worker = Worker(sys.argv[1])
	worker.start()
