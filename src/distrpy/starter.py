from tornado.ioloop import IOLoop
from threading import Thread
from .server.master import ProcMaster

class Distrpy():
    """ Service starter for API user to control the whole system. """

    def __init__(self, port):
        super(Distrpy, self).__init__()
        self.procmaster = ProcMaster(port)
        self.submit = self.procmaster.submit
        self.map = self.procmaster.map

    def close_on_finish(self):
        """ Close Distrpy and its services but doesn't close containers. """
        Thread(target=self.procmaster.close).start()
        IOLoop.current().start()

    def wait_all_job_done(self):
        Thread(target=self.procmaster.wait_no_future).start()
        IOLoop.current().start()
        print("IOLoop stopped")