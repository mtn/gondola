import sys, zmq, signal, json, tornado

from zmq.eventloop import ioloop, zmqstream

ioloop.install()

class Node(object):

    def __init__(self, name, pub, router, peers, debug):
        """
        name :: String
            The node name, from chistributed.conf
        pub :: ZMQParam
            The pub endpoint
        router :: ZMQParam
            The router endpoint
        peers :: [String]
            A list of peer names
        debug :: bool
            Flag indicating if the node will run in debug mode
        """
        self.name = name

        # Set up the loop, ZMQ sockets, and handlers
        self.loop = ioloop.IOLoop.instance()
        self.context = zmq.Context()
        self._setup_sockets(pub, router)
        self._setup_signal_handling()
        self._setup_message_handlers()

        self.debug = debug
        self.connected = False

        self.store = {}

    def log(self, msg):
        "Print log messages"
        print(">>> %10s -- %s" % (self.name, msg))

    def log_debug(self, msg):
        "Print message if debug mode is enabled (--debug)"
        if self.debug:
            print(">>> %10s -- %s" % (self.name, msg))

    def run(self):
        "Start the loop"
        self.loop.start()

    def handle_broker_message(self, msg_frames):
        "Ignore broker errors"
        pass

    def send_to_broker(self, msg):
        "Send a message to the broker"
        self.req.send_json(msg)

    def handler(self, msg_frames):
        "Handle incoming messages"

        # deal with bytes encoding
        msg_frames = [i.decode() for i in msg_frames]

        assert len(msg_frames) == 3, (
            "Multipart ZMQ message had wrong length. " "Full message contents:\n{}"
        ).format(msg_frames)

        assert msg_frames[0] == self.name

        msg = json.loads(msg_frames[2])

        if msg["type"] in self.handlers:
            handle_fn = self.handlers[msg["type"]]
            handle_fn(msg)
        else:
            self.log("Message received with unexpected type {}".format(msg["type"]))

    def hello_response_handler(self, _):
        "Response to the broker 'hello' with a 'helloResponse'"

        if not self.connected:
            self.connected = True
            self.send_to_broker(
                {"type": "helloResponse", "source": self.name}
            )
        else:
            self.log(
                "Received unexpected helloMessage after first connection, ignoring."
            )

    def _setup_sockets(self, pub, router):
        "Set up ZMQ sockets"

        # SUB socket for getting messages from broker
        self.sub_sock = self.context.socket(zmq.SUB)
        self.sub_sock.connect(pub)

        # get messages for this node
        self.sub_sock.setsockopt_string(zmq.SUBSCRIBE, self.name)

        # make a handler for recv'd msgs
        self.sub = zmqstream.ZMQStream(self.sub_sock, self.loop)
        self.sub.on_recv(self.handler)

        # REQ socket for sending messages to the broker
        self.req_sock = self.context.socket(zmq.REQ)
        self.req_sock.connect(router)
        self.req_sock.setsockopt_string(zmq.IDENTITY, self.name)

        # REQ handler, for error handling
        self.req = zmqstream.ZMQStream(self.req_sock, self.loop)
        self.req.on_recv(self.handle_broker_message)

    def _setup_signal_handling(self):
        "Setup signal handlers to gracefully shutdown"

        for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
            signal.signal(sig, self.shutdown)

    def _setup_message_handlers(self):
        self.handlers = {
            "hello": self.hello_response_handler,
        }

    def shutdown(self, _, __):
        "Shut down gracefully"

        if self.connected:
            self.loop.stop()
            self.sub_sock.close()
            self.req_sock.close()
            sys.exit(0)

    def __repr__(self):
        return "Node({})".format(self.name)
