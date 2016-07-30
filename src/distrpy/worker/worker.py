from queue import Queue
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from concurrent.futures import ThreadPoolExecutor

from ..utils.networking import Server, Client, \
                        new_stream, async_stream_task, \
                        read, write, get_ip
from ..utils.logger import Logger

log = Logger(__name__)
log.setLevel(1)

CONTAINER_NAME = 'worker_dev'

class Worker(Server):
    """ Worker class that runs inside a container.
        Receives and executes jobs from server.
        Is also a server itself.

        [Usage]
        >>> worker = Worker(host, port)
        >>> worker.start()
    """
    def __init__(self, listen_port):
        super(Worker, self).__init__()
        self.update_handlers({'add_job': self.handle_add_job,
                              'check_addr': self.handle_check_addr,
                              'pending_jobs': self.handle_pending_jobs,
                              'req_save_host': self.handle_req_save_host })
        self.job_que = Queue()
        self.master_ip = None
        self.master_port = None
        self.executor = ThreadPoolExecutor()
        self.listen_port = listen_port
        self.listen(listen_port)
        log.info("Listening on port", listen_port)

    def start(self):
        """ Start job execution loop.
            Start IOLoop.
        """
        self.executor.submit(self.exec_jobs)
        IOLoop.current().start()

    def exec_jobs(self):
        """ Job execution loop. """
        while True:
            job = self.job_que.get()
            self.executor.submit(self.start_execution, job)

    def start_execution(self, job):
        async_stream_task(self.master_ip, self.master_port, self.execute, job)

    @gen.coroutine
    def execute(self, stream, job):
        """ Execute a job and send result back to procmaster.
            TODO: catch exception in user's code and pass back to user
        """
        fid = job['fid']
        fn = job['fn']
        args = job['args']
        try:
            result = fn(*args)
        except Exception as e:
            job = {'fid': fid, 'args': args, 'except': e}
            log.debug("Exception:", e)
        else:
            job = {'fid': fid, 'args': args, 'result': result}
            log.debug("Job done")
        msg = {'op': 'job_done', 'job': job}
        try:
            yield write(stream, msg)
            reply = yield read(stream)
        except StreamClosedError:
            log.warn("Stream from worker to ProcMaster is closed, \
                     cannot send job results back.")

    @gen.coroutine
    def handle_req_save_host(self, stream, msg):
        """ Handler: Save host ip and port. """
        self.master_ip, self.master_port = msg['addr']
        log.debug("Connected to procmaster {}".format(msg['addr']))
        msg = {'reply': 'OK'}
        yield write(stream, msg)

    @gen.coroutine
    def handle_add_job(self, stream, msg):
        """ Handler: Put job to job queue on receiving a job. """
        self.job_que.put(msg['job'])
        msg = {'reply': 'OK'}
        yield write(stream, msg)

    @gen.coroutine
    def handle_pending_jobs(self, stream, msg):
        """ Handler: Send number of pedding jobs to server. """
        msg = {'count': self.job_que.qsize()}
        yield write(stream, msg)

    @gen.coroutine
    def handle_check_addr(self, stream, msg):
        """ Handler:
            Send confirm msg back to notify the other side the stream is connected.
            Only streams created by `new_stream` needs to do this step.
        """
        log.debug("Check address request from {}".format(msg['name']))
        msg = {'reply': 'OK'}
        yield write(stream, msg)


