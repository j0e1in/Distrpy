from tornado import gen
from tornado.gen import Return
from tornado import stack_context
from tornado.tcpserver import TCPServer
from tornado.tcpclient import TCPClient
from tornado.iostream import IOStream, StreamClosedError
from threading import Event
try:
    import cPickle as pickle
except ImportError:
    import pickle
import cloudpickle
import struct
import socket

from .logger import Logger

log = Logger(__name__)
log.setLevel(2)

def get_total_physical_memory():
    try:
        import psutil
        return psutil.virtual_memory().total / 2
    except ImportError:
        return 2e9

# TODO: should use container's memory limitation
MAX_BUFFER_SIZE = get_total_physical_memory()


class Server(TCPServer):
    """ A server template for master and worker to subclass.
        Inherits from `tornado.tcpserver.TCPServer`.
        All messages are sent/received from this server are in `dict` format.

        >>> def sub(stream, x, y):
        ...     return b'pong'

        >>> def add(stream, x, y):
        ...     return x + y

        >>> handlers = {'sub': sub, 'add': add}
        >>> server = Server(handlers)
        >>> server.listen(8000)

        [Param]
            handlers: dict, functions for server to use for response,
                    All handlers should accept `(stream, msg)` as arguments,
                    or `(self, stream, msg)` if the handler is a class method.
            max_buffer_size: int, max buffer size
    """
    def __init__(self, handlers={}, max_buffer_size=MAX_BUFFER_SIZE):
        super(Server, self).__init__(max_buffer_size=max_buffer_size)
        self.handlers = handlers if type(handlers) is dict else {}

    @gen.coroutine
    def handle_stream(self, stream, address):
        """ Handle new incoming connections. """
        stream.set_nodelay(True)
        ip, port = address
        # log.debug("Connection from {} to {}".format(address, type(self).__name__))
        try:
            while True:
                # receive msg
                try:
                    msg = yield read(stream)
                    # log.debug("Message from {}: {}".format(address, msg))
                except StreamClosedError:
                    log.debug("Lost connection from {} on {}".format(address, type(self).__name__))
                    break
                # except Exception as e:
                #     log.warn("handle_stream exception:", type(self).__name__, e)
                #     return
                if not isinstance(msg, dict):
                    raise TypeError("Wrong type. Expected dict, got\n" + str(msg))

                op = msg.pop('op', None) # handle no 'op' key below
                close = msg.pop('close', False)
                reply = msg.pop('reply', False)

                # handle received msg
                if op == 'close':
                    if reply:
                        yield write(stream, 'OK')
                    break
                try:
                    handler = self.handlers[op]
                except KeyError:
                    if op in [None, False, '']:
                        result = "No `op` key in msg"
                    else:
                        result = 'No handler found: {}'.format(op)
                    log.warn(result)
                else:
                    # for debugging handlers, use this snippet (without exception catching)
                    result = yield gen.maybe_future(handler(stream, msg))
                    result = {'result': result}

                    # for production use this snippet, or program will crash
                    # try:
                    #     result = yield gen.maybe_future(handler(stream, msg))
                    #     log.debug("handler result:", result)
                    #     result = {'result': result}
                    # except Exception as e:
                    #     log.warn("<exception>", e)
                    #     result = {'exception': e, 'result': ''}
                if reply:
                    try:
                        yield write(stream, result)
                    except StreamClosedError:
                        log.debug("Lost connection from {} on {}".format(address, type(self).__name__))
                        break
                if close:
                    break
        finally:
            try:
                stream.close()
            except Exception as e:
                log.warn("Failed while closing writer",  exc_info=True)

    def update_handlers(self, handlers):
        """ Add new handlers if doesn't exist.
            Replace handlers if already exist.
        """
        if type(handlers) is not dict:
            return False
        for k, v in handlers.items():
            self.handlers[k] = v
        return True

    def remove_handlers(self, handlers_name):
        """ Remove specified handlers. """
        if type(handlers) is not list:
            return False
        for k in handlers_name:
            self.handlers.pop(k)
        return True


class Client():
    """ A TCP client which has a permanent tcp connection to a server.
        Can be used as a parent class and add other functionalities.

        [Usage]
        >>> client = Client()
        # fn needs `self.stream`, so callback can only be a class method
        # fn also has to be a coroutine (use @tornado.gen.coroutine)
        # or it will block.
        >>> client.connect('localhost', 9999, client.fn)
    """
    def __init__(self):
        super(Client, self).__init__()
        self.stream = None

    @gen.coroutine
    def connect(self, ip, port, callback=None):
        """ Connect to a server. Need to be called before using stream.
            Callback has to be a coroutine (use @tornado.gen.coroutine)
        """
        if self.stream:
            self.stream.close()
            del self.stream
        self.stream = new_stream(ip, port, callback=callback)
        self.stream.set_close_callback(self.on_close)

    @gen.coroutine
    def on_close(self):
        """ Called when current stream is closed.
            Can be implemented in subclass.
        """
        pass



def dumps(x):
    """ Manage between cloudpickle and pickle

    1.  Try pickle
    2.  If it is short then check if it contains __main__
    3.  If it is long, then first check type, then check __main__
    """
    try:
        result = pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        if len(result) < 1000:
            if b'__main__' in result:
                return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
            else:
                return result
        else:
            if isinstance(x, pickle_types) or b'__main__' not in result:
                return result
            else:
                return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
    except:
        try:
            return cloudpickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            log.warn("Failed to serialize {}".format(x))
            raise

def loads(x):
    try:
        return pickle.loads(x)
    except Exception:
        log.warn("Failed to deserialize {}".format(x))
        raise

def new_stream(ip, port, callback=None):
    """ Create, connect and return a stream in blocking mode.
        This is for longterm connection use, for async connection
        see `async_stream_task`
    """
    # TODO: handle exception on IOStream.connect()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    stream = IOStream(s)
    stream.connect((ip, port), callback=callback)
    return stream

MAX_CONNS = 100
stream_count = 0
not_too_many_stream = Event()
not_too_many_stream.set()

@gen.coroutine
def async_stream_task(ip, port, callback, *args, **kwargs):
    """ 1. Execute callback on connection established.
        2. Passes stream to callback.
        3. Close stream when callback is done.
        - Callback need to accept `stream` as its first argument
        or second if `self` is the first argument.
        - callback needs to be a coroutine
        - It's better to add {'close': True} to the last sent msg
        in callback to avoid "lost connection" warning on server side.
    """
    # TODO: handle exception on IOStream.connect()
    global stream_count
    global not_too_many_stream
    not_too_many_stream.wait()
    stream = yield TCPClient().connect(ip, port)
    stream_count += 1
    if not_too_many_stream.is_set() and stream_count > MAX_CONNS:
        not_too_many_stream.clear()
    yield callback(stream, *args, **kwargs)
    stream.close()
    stream_count -= 1
    if not not_too_many_stream.is_set() and stream_count < MAX_CONNS:
        not_too_many_stream.set()
    del stream

def wrap_cb(cb):
    """ Helper function for wrapping callback for async functions. """
    return stack_context.wrap(cb) if cb else None

@gen.coroutine
def read(stream):
    """ Read a message from a stream.
        Always receives pickled dict.
        This assumes msg len is small (< 1e^6 bytes)
    """
    length = yield stream.read_bytes(8) # read `length` of the msg (uint32)
    length = struct.unpack('Q', length)[0]
    if length:
        msg = yield stream.read_bytes(length)
    else:
        msg = b''
    msg = loads(msg)
    raise Return(msg)

@gen.coroutine
def write(stream, msg):
    """ Write a message to a stream.
        Always sends pickled dict.
        This assumes msg len is small. (< 1e^6 bytes)
    """
    if not isinstance(msg, dict):
        if isinstance(msg, str):
            msg = {'op': 'str', 'msg': msg}
        else:
            msg = {'op': 'obj', 'msg': msg}
    msg = dumps(msg)
    length = struct.pack('Q', len(msg)) # uint32
    stream.write(length)
    stream.write(msg)

def get_ip():
    """ Get IP address of `eth0` """
    import netifaces as ni
    interface = None
    if 'docker0' in ni.interfaces() and 2 in ni.ifaddresses('docker0'):
        interface = 'docker0'
    elif 'eth0' in ni.interfaces() and 2 in ni.ifaddresses('eth0'):
        interface = 'eth0'
    elif 'lo' in ni.interfaces() and 2 in ni.ifaddresses('lo'):
        interface = 'lo'
    elif 'lo0' in ni.interfaces() and 2 in ni.ifaddresses('lo0'):
        interface = 'lo0'
    else:
        log.error("No valid interface name can be used")
    log.debug("netifaces.interfaces:", ni.interfaces())
    log.debug("interface:", interface)
    ip = ni.ifaddresses(interface)[2][0]['addr']
    log.debug("Get IP {}".format(ip))
    return ip
