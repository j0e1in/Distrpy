DEBUG = 1
INFO = 2
WARN = 3
ERROR = 4
CRITICAL = 5

class Logger():

    def __init__(self, name):
        super(Logger, self).__init__()
        self.name = name
        self.lvl = WARN # default level

    def setLevel(self, lvl):
        self.lvl = lvl

    def debug(self, msg, *args):
        if self.lvl <= DEBUG:
            print("=DEBUG= ({}) :".format(self.name), msg, *args)

    def info(self, msg, *args):
        if self.lvl <= INFO:
            print("=INFO= ({}) :".format(self.name), msg, *args)

    def warn(self, msg, *args):
        if self.lvl <= WARN:
            print("=WARN= ({}) :".format(self.name), msg, *args)

    def error(self, msg, *args):
        if self.lvl <= ERROR:
            print("=ERR= ({}) :".format(self.name), msg, *args)

    def critical(self, msg, *args):
        if self.lvl <= CRITICAL:
            print("=CRITICAL= ({}) :".format(self.name), msg, *args)

def trace():
    import pdb; pdb.set_trace()