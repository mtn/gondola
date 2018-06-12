"A Raft Node"

import sys
import signal
import json
from time import time
from enum import Enum, auto
from random import randint

from zmq.eventloop import ioloop, zmqstream
import zmq

from rpc import RPC, RequestVote, VoteResponse

# pylint: disable=too-many-instance-attributes
# pylint: disable=missing-docstring
# pylint: disable=bad-continuation

ioloop.install()

# TODO conf
TIMEOUT_INF = 150
TIMEOUT_SUP = 300


class Role(Enum):
    "Possible node states"
    Follower = auto()
    Candidate = auto()
    Leader = auto()


class Node(object):
    "Raft node"

    # pylint: disable=too-many-arguments
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
        self._timeout = None

        self.debug = debug
        self.connected = False
        self.peers = peers
        self.role = None

        # Persistent state
        self.current_term = 0  # latest term the server has seen
        self.voted_for = None  # candidate_id that received vote in current term
        self.ledger = []  # ledger entries for state machine
        self.store = {}  # store that is updated as ledger entries are commited

        # Volatile state
        self.commit_index = 0  # index of the highest ledger entry known to be commited
        self.last_applied = 0  # index of the highest ledger entry applied

        # Volatile state; only used when acting as a leader
        self.next_index = []  # index of the next ledger entry to send each server
        self.match_index = []  # index of the highest ledger entry known to be replicated

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

        msgs = []

        if isinstance(msg, RPC):
            msgs = msg.serialize()
        else:
            msgs = [msg]

        for to_transmit in msgs:
            self.req.send_json(to_transmit)

    def handler(self, msg_frames):
        "Handle incoming messages"

        # deal with bytes encoding
        msg_frames = [i.decode() for i in msg_frames]

        assert len(msg_frames) == 3, (
            "Multipart ZMQ message had wrong length. Full message contents:\n{}"
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
            self.send_to_broker({"type": "helloResponse", "source": self.name})

            self.role = self.become_follower()
        else:
            self.log(
                "Received unexpected helloMessage after first connection, ignoring."
            )

    def append_entries_handler(self, msg):
        "Handle append entry requests"
        pass

    def append_response_handler(self, msg):
        "Handle append entry responses (as the leader)"
        pass

    def request_vote_handler(self, msg):
        "Handle request vote requests"
        self.log_debug("Handling vote request")

        if self.current_term < msg["term"]:
            self.step_down(msg["term"])

        granted = False

        if (
            self.current_term == msg["term"]
            and self.voted_for in [None, msg["source"]]
            and (
                msg["lastLogTerm"] > self.log_term(len(self.ledger))
                or (
                    msg["lastLogTerm"] == self.log_term(len(self.ledger))
                    and msg["lastLogIndex"] >= len(self.ledger)
                )
            )
        ):

            self.voted_for = msg["source"]
            granted = True
            self.set_timeout()

        self.send_to_broker(
            VoteResponse(self.name, [msg["source"]], self.current_term, granted)
        )

    def vote_response_handler(self, msg):
        "Handle request vote responses"
        self.log("handling vote response")

    def become_follower(self):
        "Transition to follower role and start an election timer"

        self.role = Role.Follower
        self.set_timeout()

    def start_election(self):
        "Start an election by requesting a vote from each node"

        self.log("Starting an election")

        if self.ledger:
            last_log_term = self.ledger[-1].term
        else:
            last_log_term = 0

        self.send_to_broker(
            RequestVote(
                self.name,
                self.peers,
                self.current_term,
                len(self.ledger),
                last_log_term,
            )
        )

    def set_timeout(self):
        "Add an election timeout"

        # Clear any pending timeout
        self.clear_timeout()

        interval = randint(TIMEOUT_INF, TIMEOUT_SUP) / 1000
        self._timeout = self.loop.add_timeout(time() + interval, self.start_election)

    def clear_timeout(self):
        "Clear a pending timeout"

        if not self._timeout:
            return

        self.loop.remove_timeout(self._timeout)
        self._timeout = None

    def step_down(self, new_term):
        "Step down as leader"

        self.current_term = new_term
        self.role = Role.Follower
        self.voted_for = None

        self.set_timeout()

    def log_term(self, ind):
        "A safe accessor for indexing into the ledger"

        if ind < 0 or ind >= len(self.ledger):
            return 0
        return self.ledger[ind - 1].term

    def _setup_sockets(self, pub, router):
        "Set up ZMQ sockets"
        # pylint: disable=no-member

        self.sub_sock = self.context.socket(zmq.SUB)
        self.sub_sock.connect(pub)

        self.sub_sock.setsockopt_string(zmq.SUBSCRIBE, self.name)

        self.sub = zmqstream.ZMQStream(self.sub_sock, self.loop)
        self.sub.on_recv(self.handler)

        self.req_sock = self.context.socket(zmq.REQ)
        self.req_sock.connect(router)
        self.req_sock.setsockopt_string(zmq.IDENTITY, self.name)

        self.req = zmqstream.ZMQStream(self.req_sock, self.loop)
        self.req.on_recv(self.handle_broker_message)

    def _setup_signal_handling(self):
        "Setup signal handlers to gracefully shutdown"

        for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
            signal.signal(sig, self.shutdown)

    def _setup_message_handlers(self):
        self.handlers = {
            "hello": self.hello_response_handler,
            "requestVote": self.request_vote_handler,
            "voteResponse": self.vote_response_handler,
            "appendEntries": self.append_entries_handler,
            "appendResponse": self.append_response_handler,
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
