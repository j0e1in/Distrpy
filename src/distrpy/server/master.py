# from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Event, Thread, active_count
from tornado import gen
from tornado.iostream import StreamClosedError
from tornado.ioloop import IOLoop
from queue import Queue
import uuid

from ..utils.tools import glob_executor
from ..utils.cluster import DockerManager
from ..utils.logger import Logger, trace
from ..utils.networking import Server, read, write, get_ip, \
                            new_stream, async_stream_task

TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
CONTAINER_PREFIX = 'distrpy_worker_'
MAX_FUTURES = 300

log = Logger(__name__)
log.setLevel(1) # DEBUG


class ProcMaster():
    """ Interface to users.
        In charge of container management.

        [param]
            port: port for Scheduler server to listen to
    """
    def __init__(self, port):
        super(ProcMaster, self).__init__()
        self.port = port
        self.future_executor = ThreadPoolExecutor(max_workers=MAX_FUTURES)
        self.docker = DockerManager(CONTAINER_PREFIX)
        self.schd = Scheduler(self.docker)
        self.schd.listen(self.port)
        log.info("Listening on port {}".format(self.port))
        self._connect_workers()
        print("====================================")
        print("         ProcMaster Started         ")
        print("====================================")

    def submit(self, fn, arg=None, callback=None, *cb_args, cb_per_result=False):
        """ Singular version of map. Queue a job only. """
        if arg is not None and not isinstance(arg, tuple):
            return False
        return self.map(fn, [arg], callback, *cb_args, cb_per_result)

    def map (self, fn, args, callback=None, *cb_args, cb_per_result=False):
        """ Main API for user to pass jobs to ProcMaster.
            ProcMaster will unpack the jobs and add them
            to the ProcMaster's job queue,

            [Param]
                fn: function to be mapped to workers
                args: list of tuples, a list of args to be
                    passed to the function
        """
        from types import GeneratorType
        if not isinstance(args, GeneratorType):
            if not isinstance(args, list) \
            or len(args) <= 0 \
            or not isinstance(args[0], tuple):
                log.warn("args type:", type(args))
                log.warn("args[0] type:", type(args[0]))
                return False

        time = datetime.now().strftime(TIMESTAMP_FORMAT)
        fid = uuid.uuid1()
        future = Future(fid, callback, *cb_args, cb_per_result=cb_per_result)
        for i, arg in enumerate(args):
            new_job =  {'fid': fid,
                        'time': time,
                        'fn': fn,
                        'args': arg }
            self.schd.queue_job(new_job)
            future.add_job(new_job)
        self.schd.sf.add_future(fid, future)
        return True

    @gen.coroutine
    def wait_no_future(self):
        """ Wait for all futures are cleared. """
        self.schd.wait_no_future.set()
        self.schd.no_future.wait()
        self.schd.wait_no_future.clear()
        IOLoop.current().stop()

    @gen.coroutine
    def close(self):
        """ Close ProcMaster and other threads. """
        self.schd.no_new_job.set()
        self.schd.all_done.wait()
        self.schd.queue_job('END') # to end job dispatcher job assignment loop
        self.docker.shutdown()
        log.info("Finished jobs exectution, waiting for executor to shutdown...")
        glob_executor.shutdown()
        IOLoop.current().stop()
        log.info("Closing program...")

    @gen.coroutine
    def _connect_workers(self):
        """ Sends host ip and port to worker. """
        for name, c in self.docker.containers.items():
            p = get_worker_port(c.ports, name)
            if not p:
                log.warn("Worker port is invalid or not exposed to host.")
                return
            ip = c.ip
            addr = (ip, p)
            async_stream_task(*addr, self._req_save_host, name, addr)

    @gen.coroutine
    def _req_save_host(self, stream, name, worker_addr):
        host_addr = (get_ip(), self.port)
        msg = {'op': 'req_save_host', 'addr': host_addr}
        try:
            yield write(stream, msg)
            reply = yield read(stream)
        except StreamClosedError:
            log.warn("Stream _req_save_host to worker {} is closed, \
                     cannot connect to the worker.".format(name))
        else:
            if reply['reply'] == 'OK':
                log.info("Connected to worker: {}".format(name))
                self.schd.sf.add_addr(name, worker_addr)



class Scheduler(Server):
    """ A layer between ProcMaster and Workers.
        In charge of scheduling and data transmission.
    """
    def __init__(self, docker):
        super(Scheduler, self).__init__()
        self.update_handlers({'job_done': self.handle_job_done })
        self.sf = SchedulerFactory(docker)
        self.all_done = self.sf.all_done
        self.no_new_job = self.sf.no_new_job
        self.no_future = self.sf.no_future
        self.wait_no_future = self.sf.wait_no_future
        self.job_dispatcher = JobDispatcher(self.sf)
        glob_executor.submit(self.job_dispatcher.run)

    def queue_job(self, job):
        self.sf.job_q.put_nowait(job)

    @gen.coroutine
    def handle_job_done(self, stream, msg):
        job = msg['job']
        msg = {'reply': 'OK'}
        yield write(stream, msg)
        future = self.sf.get_future(job['fid'])
        if not future:
            log.error("get_future return None")
        else:
            future.set_result(job)



class JobDispatcher():
    """ Sends jobs to workers in a different thread. """
    def __init__(self, scheduler_factory):
        super(JobDispatcher, self).__init__()
        self.sf = scheduler_factory

    def run(self):
        """ Consumes job  Sends jobs to workers.
            This function is exectued asyncronously.
        """
        while True:
            self.sf.has_worker.wait() # wait until has connections from workers
            job = self.sf.job_q.get() # wait until is not empty
            if job == 'END':
                break
            self._assign_job(job)

    @gen.coroutine
    def _assign_job(self, job):
        lowest_cpu = 100 # in percentage
        target = None # name of the container to assign the job
        # for n, c in self.sf.docker.containers.items():
        #     if c.cpu < lowest_cpu and n in self.sf.addrs.keys():
        #         target = n
        if not target:
            # assign to a random worker
            import random
            while True:
                t = random.randint(0, len(self.sf.docker.containers)-1)
                target = list(self.sf.docker.containers.keys())[t]
                if target in self.sf.addrs.keys():
                    break
        # send job to the target worker
        log.debug("Job to {}".format(target))
        async_stream_task(*self.sf.addrs[target], self.send_job, target, job)

    @gen.coroutine
    def send_job(self, stream, target, job):
        try:
            msg = {'op': 'add_job', 'job': job}
            yield write(stream, msg)
            reply = yield read(stream) # will be read from here or in handle_stream ?
        except StreamClosedError:
            log.warn("Worker {} stream is closed, cannot assign job, \
                    pushing job `{}` back to job queue".format(target, job))
            self.sf.job_q.put_nowait(job)
        else:
            # TO FIX: some job reply may not be received
            if reply['reply'] != 'OK':
                log.debug("assign job reply is not OK", reply)



class SchedulerFactory():
    """ Shares data among classes in Scheduler.
        This is needed since there are many threads running in Scheduler.
    """
    def __init__(self, docker):
        super(SchedulerFactory, self).__init__()
        self.docker = docker
        self.addrs = {} # ip & port of workers
        self.has_worker = Event()
        self.all_done = Event()
        self.no_new_job = Event()
        self.no_future = Event()
        self.wait_no_future = Event()
        self.job_q = Queue()
        self.futures = {}

    def add_addr(self, name, addr):
        self.addrs[name] = addr
        if not self.has_worker.is_set() and name in self.docker.containers.keys():
            self.has_worker.set()
            log.info("{} workers are connected".format(len(self.addrs)))

    def remove_addr(self, name):
        try:
            addr = self.addrs.pop(name)
        except KeyError:
            log.warn("addr `{}` doesn't exist".format(name))
            return
        else:
            return addr

    def add_future(self, fid, future):
        self.futures[fid] = future
        if self.no_future.is_set():
            self.no_future.clear()
        glob_executor.submit(self._wait_future_finish, future)

    def get_future(self, fid):
        if fid in self.futures:
            return self.futures[fid]
        else:
            return None

    def remove_future(self, fid):
        if fid in self.futures:
            return self.futures.pop(fid)
        else:
            return None

    def _wait_future_finish(self, future):
        future.finished.wait()
        self.remove_future(future.fid)
        if len(self.futures) == 0:
            if self.no_new_job.is_set():
                self.all_done.set()
            elif self.wait_no_future.is_set():
                self.no_future.set()



class Future():
    """ Future class for user to get result/exception of jobs. """
    def __init__(self, fid, callback=None, *args, cb_per_result=False):
        self.fid = fid
        self._fn = None
        if callback:
            if cb_per_result:
                self.result_handler = CallbackMulti(self, callback, *args)
            else:
                self.result_handler = CallbackOnce(self, callback, *args)
        else:
            self.result_handler = None
        self._jobs = {}
        self.finished = Event()
        self.cb_per_result = cb_per_result

    def add_job(self, job):
        if self._fn is None:
            self._fn = job['fn']
        else:
            if job['fn'] != self._fn:
                # function of job doesn't match this future's
                return False
        self._jobs[job['args']] = job
        self.result_handler.set_result(job['args'], 'result', None)
        return True

    def set_result(self, job):
        if 'result' in job:
            self.result_handler.set_result(job['args'], 'result', job['result'])
        elif 'except' in job:
            self.result_handler.set_result(job['args'], 'except', job['except'])


class CallbackOnce():
    """ Call callback if all results are available. """
    def __init__(self, future, cb, *args):
        super(CallbackOnce, self).__init__()
        self.future = future
        self.cb = cb
        self._results = {}
        self._results_available = Event()
        self.job_count = 0
        self.finished_job_count = 0
        glob_executor.submit(self._wait_result, cb, *args)

    def set_result(self, args, result_type, result):
        self._results[args] = { result_type: result }
        if result:
            self.finished_job_count += 1
        else:
            self.job_count += 1
        if self._all_job_done():
            self._results_available.set()

    def _wait_result(self, callback, *args):
        """ Wait for all results are available and pass them to callback.
            `results` is a generator in the form `(args, result)`.
        """
        self._results_available.wait()
        results = self._get_results()
        callback(results, *args)
        self.future.finished.set()

    def _get_results(self):
        for args, result in self._results.items():
            yield (args, result)

    def _all_job_done(self):
        return self.finished_job_count == self.job_count


class CallbackMulti():
    """ Call callback whenever a result is available. """
    def __init__(self, future, cb, *args):
        super(CallbackMulti, self).__init__()
        self.future = future
        self.cb = cb
        self._results = {}
        self._reuslt_q = Queue()
        self.job_count = 0
        self.finished_job_count = 0
        glob_executor.submit(self._wait_result, cb, *args)

    def set_result(self, args, result_type, result):
        self._results[args] = { result_type: result }
        if result:
            self._reuslt_q.put_nowait((args, self._results[args]))
        else:
            # result == None, adding a job to _results
            self.job_count += 1

    def _wait_result(self ,callback, *args):
        while True:
            arg_result = self._reuslt_q.get()
            callback(arg_result, *args)
            self.finished_job_count += 1
            if self._all_job_done():
                self.future.finished.set()
                break

    def _all_job_done(self):
        return self.finished_job_count == self.job_count


def get_worker_port(container_ports, name):
    # assume only one port opened in host
    for k, pp in container_ports.items():
        if pp:
            for p in pp:
                if 'HostPort' in p:
                    return p['HostPort']
    return None
