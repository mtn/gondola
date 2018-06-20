"A Raft Node"

import sys
import signal
import json
from time import time
from enum import Enum, auto
from random import randint
from math import floor

from rpc import RequestVote, VoteResponse, AppendEntries, AppendResponse
from client_response import SetResponse, GetResponse
from orchestrator import Orchestrator
from log import Log

# pylint: disable=too-many-instance-attributes
# pylint: disable=missing-docstring
# pylint: disable=bad-continuation


# TODO conf, make these shorter
TIMEOUT_INF = 2000
TIMEOUT_SUP = 3000
HEARTBEAT_INF = 250
HEARTBEAT_SUP = 1000


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

        self.election_timeout = None

        self._setup_signal_handling()
        self._setup_message_handlers()
        self.orchestrator = Orchestrator(self, self.name, debug, pub, router)

        self.connected = False
        self.peers = peers
        self.role = None
        self.leader = None

        # Persistent state
        self.current_term = 0  # latest term the server has seen
        self.voted_for = None  # candidate_id that received vote in current term
        self.log = Log()  # log entries for state machine
        self.store = {}  # store that is updated as log entries are commited

        # Volatile state
        self.commit_index = 0  # index of the highest log entry known to be commited
        self.last_applied = 0  # index of the highest log entry applied

        # Volatile state; only used when acting as a leader or candidate
        # Invalidated on each new term
        self.init_term_state()

    def handle_broker_message(self, msg_frames):
        "Ignore broker errors"
        pass

    def handler(self, msg_frames):
        "Handle incoming messages"

        msg_frames = [i.decode() for i in msg_frames]

        assert len(msg_frames) == 3, (
            "Multipart ZMQ message had wrong length. Full message contents:\n{}"
        ).format(msg_frames)

        assert msg_frames[0] == self.name

        msg = json.loads(msg_frames[2])

        if msg["type"] in self.handlers:
            # Messages from before we've said hello are dropped
            # Failing to do so results in errors with chistributed
            if self.connected or msg["type"] == "hello":
                handle_fn = self.handlers[msg["type"]]
                handle_fn(msg)
            else:
                if "type" in msg and "source" in msg:
                    self.orchestrator.log(
                        "Ignoring message of type {} from source {}".format(
                            msg["type"], msg["source"]
                        )
                    )
        else:
            self.orchestrator.log(
                "Message received with unexpected type {}".format(msg["type"])
            )

    def hello_request_handler(self, _):
        "Response to the broker 'hello' with a 'helloResponse'"

        if not self.connected:
            self.connected = True
            self.orchestrator.send_to_broker(
                {"type": "helloResponse", "source": self.name}
            )

            self.orchestrator.log_debug("I'm {} and I've said hello".format(self.name))

            self.become_follower()
        else:
            self.orchestrator.log(
                "Received unexpected helloMessage after first connection, ignoring."
            )

    def append_entries_handler(self, msg):
        "Handle append entry requests"

        # Step down if this node is the leader and is out of date
        if self.current_term < msg["term"]:
            self.step_down(msg["term"])

        # If the term isn't current, we return early
        if self.current_term > msg["term"]:
            self.orchestrator.send_to_broker(
                AppendResponse(
                    self.name,
                    [msg["source"]],
                    self.current_term,
                    False,
                    self.commit_index,
                )
            )
            return

        self.leader = msg["source"]
        self.role = Role.Follower

        # Reset the election timeout
        self.set_election_timeout()

        # If there is a previous term, then it should match the one in the node's log
        success = (
            msg["prevLogTerm"] is None
            or self.log.term(msg["prevLogIndex"]) == msg["prevLogTerm"]
        )

        if success:
            index = msg["prevLogIndex"]

            for entry in msg["entries"]:
                index += 1

                if not self.log.term(index) == entry.term:
                    self.log.update_until(index, entry)
                self.commit_index = min(msg["leaderCommit"], index)
        else:
            index = 0

        self.orchestrator.send_to_broker(
            AppendResponse(
                self.name,
                [msg["source"]],
                self.current_term,
                success,
                self.commit_index,
            )
        )

    def append_response_handler(self, msg):
        "Handle append entry responses (as the leader)"

        peer = msg["source"]

        if self.current_term < msg["term"]:
            self.step_down(msg["term"])

        elif self.role == Role.Leader and self.current_term == msg["term"]:
            if msg["success"]:
                self.match_index[peer] = msg["matchIndex"]
                self.next_index[peer] = msg["matchIndex"] + 1
            else:
                self.next_index[peer] = max(0, self.next_index[peer] - 1)

    def request_vote_handler(self, msg):
        "Handle request vote requests"
        self.orchestrator.log_debug(
            "Handling vote request from {}".format(msg["source"])
        )

        if self.current_term < msg["term"]:
            self.step_down(msg["term"])

        granted = False

        term_is_current = self.current_term <= msg["term"]
        can_vote = self.voted_for in [None, msg["source"]]

        # If there's nothing in our log, they must be at least as up-to-date
        if not self.log.term():
            log_up_to_date = True
        else:
            # If we have log entries and they don't, they must be out of date
            if msg["lastLogTerm"] is None:
                log_up_to_date = False
            else:
                log_up_to_date = msg["lastLogTerm"] > self.log.term() or (
                    msg["lastLogTerm"] == self.log.term()
                    and msg["lastLogIndex"] >= len(self.log)
                )

        if term_is_current and can_vote and log_up_to_date:
            self.voted_for = msg["source"]
            granted = True
            self.set_election_timeout()

        self.orchestrator.send_to_broker(
            VoteResponse(self.name, [msg["source"]], self.current_term, granted)
        )

    def vote_response_handler(self, msg):
        "Handle request vote responses"
        self.orchestrator.log_debug(
            "Handling vote response from {}".format(msg["source"])
        )

        # If the responder has a more current term, we're stale
        if self.current_term < msg["term"]:
            self.step_down(msg["term"])

        if self.role == Role.Candidate and self.current_term == msg["term"]:
            self.clear_timeout(name=msg["source"])
            self.vote_granted[msg["source"]] = msg["voteGranted"]
        else:
            self.orchestrator.log_debug("Ignoring vote, they were stale")

        self.orchestrator.log_debug("Votes : {}".format(self.vote_granted))
        self.orchestrator.log_debug(
            "Votes received: {}".format(sum(self.vote_granted.values()))
        )
        if self.role == Role.Candidate and sum(self.vote_granted.values()) >= floor(
            len(self.peers) / 2 + 1
        ):
            self.become_leader()

    def set_request_handler(self, msg):
        "Handle client set requests"

        # Forward request to leader, if we're not it
        if self.leader is not None and not self.name == self.leader:
            msg["destination"] = self.leader
            msg["type"] = "setRedirect"
            self.orchestrator.send_to_broker(msg)
            return

        if self.leader is None:
            self.orchestrator.send_to_broker(
                SetResponse(
                    self.name,
                    msg["id"],
                    error="Could not set {} to {} -- currently not in contact with a leader".format(
                        msg["key"], msg["value"]
                    ),
                )
            )
            return

        # We must be the leader
        # self.log.append_entries(SetResponse())

    def get_request_handler(self, msg):
        "Handle client get requests"

        # Forward request to leader, if we're not it
        if self.leader is not None and not self.name == self.leader:
            msg["destination"] = self.leader
            msg["type"] = "getRedirect"
            self.orchestrator.send_to_broker(msg)
            return

        if self.leader is None:
            self.orchestrator.send_to_broker(
                GetResponse(
                    self.name,
                    msg["id"],
                    error="Could not get {} -- currently not in contact with a leader".format(
                        msg["key"]
                    ),
                )
            )
            return

        # We must be the leader
        if msg["key"] in self.store:
            self.orchestrator.send_to_broker(
                GetResponse(
                    self.name, msg["id"], key=msg["key"], value=self.store[msg["key"]]
                )
            )
        else:
            self.orchestrator.send_to_broker(
                GetResponse(
                    self.name, msg["id"], error="No such key: {}".format(msg["key"])
                )
            )

    def become_candidate(self):
        "Start an election by requesting a vote from each node"
        self.orchestrator.log_debug("Starting an election")

        if self.role in [Role.Follower, Role.Candidate]:
            self.set_election_timeout()

            self.current_term += 1  # Increment term

            self.role = Role.Candidate

            self.init_term_state()

            # Vote for self
            self.voted_for = self.name
            self.vote_granted[self.name] = True

            self.orchestrator.send_to_broker(
                RequestVote(
                    self.name,
                    self.peers,
                    self.current_term,
                    len(self.log),
                    self.log.term(),
                )
            )

    def become_follower(self):
        "Transition to follower role and start an election timer"
        self.orchestrator.log_debug("Becoming a follower")

        self.role = Role.Follower
        self.set_election_timeout()

    # pylint: disable=attribute-defined-outside-init
    def become_leader(self):
        "Transition to a leader state. Assumes votes have been checked by caller."
        self.orchestrator.log_debug("Won election with votes, becoming leader")

        # Clear election timeout, if one is set
        self.clear_timeout()

        self.role = Role.Leader
        self.leader = self.name

        self.match_index = {p: 0 for p in self.peers}
        self.next_index = {p: self.commit_index + 1 for p in self.match_index}

        self.send_append_entries()

    def send_append_entries(self):
        """
        Send out append entries and schedule next heartbeat timeout.
        The leader always sends what it believes to be the entire diff.
        """

        assert self.role == Role.Leader

        for peer in self.peers:
            prev_index = self.next_index[peer]

            self.set_rpc_timeout(peer)

            # After the rpc, the node will have the entire log
            self.next_index[peer] = len(self.log)

            self.orchestrator.send_to_broker(
                AppendEntries(
                    self.name,
                    [peer],
                    self.current_term,
                    self.leader,
                    self.next_index[peer] - 1,
                    self.log.term(self.next_index[peer] - 1),
                    self.log.entries[prev_index : self.next_index[peer]],
                    self.commit_index,
                )
            )

    def advance_commit_index(self):
        "Advance the commit index based on the current majorities"

        pass

    def set_election_timeout(self):
        "Set the election timeout. If one was already set, override it."

        # Clear any pending timeout
        self.clear_timeout()

        # TODO revert; for testing, make node-1 deterministically win the first election
        # alternatively, let nodes supply their own rng and default to randint
        if self.name == "node-1":
            interval = randint(TIMEOUT_INF, TIMEOUT_INF) / 1000
        else:
            interval = randint(TIMEOUT_INF + 1, TIMEOUT_SUP) / 1000

        self.election_timeout = self.orchestrator.loop.add_timeout(
            time() + interval, self.become_candidate
        )

    def set_rpc_timeout(self, name):
        "Set an RPC (heartbeat) timeout"
        assert name in self.rpc_timeouts

        # Clear any pending timeout
        self.clear_timeout(name)

        interval = randint(HEARTBEAT_INF, HEARTBEAT_SUP) / 1000
        self.rpc_timeouts[name] = self.orchestrator.loop.add_timeout(
            time() + interval, self.send_append_entries
        )

    def clear_timeout(self, name=None):
        """
        Clear a pending timeout.
        If no arguments are passed, the election timeout is reset.
        Otherwise, use the name to index into the RPC timeouts.
        """

        if name:
            assert name in self.rpc_timeouts

            if self.rpc_timeouts[name]:
                self.orchestrator.loop.remove_timeout(self.rpc_timeouts[name])
                self.rpc_timeouts[name] = None

            return

        if not self.election_timeout:
            return

        self.orchestrator.loop.remove_timeout(self.election_timeout)
        self.election_timeout = None

    def step_down(self, new_term):
        "Step down as leader"

        self.current_term = new_term
        self.role = Role.Follower
        self.voted_for = None

        if not self.election_timeout:
            self.set_election_timeout()

    def init_term_state(self):
        """
        Initialize state that is tracked for a single term (including as leader).
        We can initialize leader state now too because if the node becomes a leader,
        none of it will have been able to change.
        """

        # Index of the next log entry to send each server
        # This value gets walked back, if a node responds saying they are further behind
        # However, there will be a mojority this far
        self.next_index = {p: self.commit_index + 1 for p in self.peers}

        # Index of the highest log entry known to be replicated
        self.match_index = {p: 0 for p in self.peers}

        # True for each peer that has granted its vote
        self.vote_granted = {p: False for p in self.peers}

        # Timeouts for peer rpcs (send another rpc when triggered)
        self.rpc_timeouts = {p: None for p in self.peers}

    def _setup_signal_handling(self):
        "Setup signal handlers to gracefully shutdown"

        for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
            signal.signal(sig, self.shutdown)

    def _setup_message_handlers(self):
        self.handlers = {
            "hello": self.hello_request_handler,
            "requestVote": self.request_vote_handler,
            "voteResponse": self.vote_response_handler,
            "appendEntries": self.append_entries_handler,
            "appendResponse": self.append_response_handler,
            "set": self.set_request_handler,
            "setRedirect": self.set_request_handler,
            "get": self.get_request_handler,
            "getRedirect": self.get_request_handler,
        }

    def run(self):
        "Start the loop"
        self.orchestrator.loop.start()

    def shutdown(self, _, __):
        "Shut down gracefully"

        if self.connected:
            self.orchestrator.loop.stop()
            self.orchestrator.sub_sock.close()
            self.orchestrator.req_sock.close()
            sys.exit(0)

    def __repr__(self):
        return "Node({})".format(self.name)
