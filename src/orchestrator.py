"""
Object for configuration-related node methods (eg. logging, sending messages).
Also, avoids name collision of self.log.
Design somewhat based on zatt (github.com/simonacca/zatt).
"""

from zmq.eventloop import ioloop, zmqstream
import zmq

from message import Message

# pylint: disable=too-many-instance-attributes
# pylint: disable=missing-docstring
# pylint: disable=too-many-arguments

ioloop.install()


class Orchestrator(object):
    def __init__(self, node, name, debug, pub, router):
        self.node = node
        self.name = name
        self.debug = debug

        self.connected = False

        # Set up the loop, ZMQ sockets, and handlers
        self.loop = ioloop.IOLoop.instance()
        self.context = zmq.Context()
        self._setup_sockets(pub, router)

    def log(self, msg):
        "Print log messages"
        if self.debug:
            from time import time

            print(">>> {} {} -- {}".format(time(), self.name, msg))
        else:
            print(">>> %10s -- %s" % (self.name, msg))

    def log_debug(self, msg):
        "Print message if debug mode is enabled (--debug)"
        from time import time

        if self.debug:
            print(">>> {} {} -- {}".format(time(), self.name, msg))

    def _setup_sockets(self, pub, router):
        "Set up ZMQ sockets"
        # pylint: disable=no-member

        self.sub_sock = self.context.socket(zmq.SUB)
        self.sub_sock.connect(pub)

        self.sub_sock.setsockopt_string(zmq.SUBSCRIBE, self.name)

        self.sub = zmqstream.ZMQStream(self.sub_sock, self.loop)
        self.sub.on_recv(self.node.handler)

        self.req_sock = self.context.socket(zmq.REQ)
        self.req_sock.connect(router)
        self.req_sock.setsockopt_string(zmq.IDENTITY, self.name)

        self.req = zmqstream.ZMQStream(self.req_sock, self.loop)
        self.req.on_recv(self.node.handle_broker_message)

    def send_to_broker(self, msg):
        "Send a message to the broker. Non-RPC objects aren't examined."

        msgs = []

        if isinstance(msg, Message):
            msgs = msg.serialize()
        else:
            msgs = [msg]

        for to_transmit in msgs:
            try:
                if not (
                    (
                        to_transmit["type"] == "appendEntries"
                        and not to_transmit["entries"]
                    )
                    or to_transmit["type"] == "appendResponse"
                ):
                    self.log_debug("Sending {}".format(to_transmit))
            except:
                print(type(to_transmit))
                print(to_transmit)
            self.req.send_json(to_transmit)
