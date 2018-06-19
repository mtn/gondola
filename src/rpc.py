"RPC objects to simplify message passing"

# pylint: disable=too-few-public-methods
# pylint: disable=too-many-arguments
# pylint: disable=missing-docstring
# pylint: disable=bad-continuation


class RPC(object):
    def __init__(self, src, dests):
        """
        src :: String
            The node name of the source
        dests :: [String]
            The node names of the destinations
        """
        self.src = src
        self.dests = dests

    def serialize(self):
        "Serialize into a list of messages that the broker can transmit"


class RequestVote(RPC):
    "Invoked by candidates to gather votes"

    def __init__(self, src, dests, term, last_log_index, last_log_term):
        RPC.__init__(self, src, dests)

        self.term = term
        self.last_index = last_log_index
        self.last_term = last_log_term

    def serialize(self):
        msgs = []

        for dest in self.dests:
            msgs.append(
                {
                    "type": "requestVote",
                    "source": self.src,
                    "destination": dest,
                    "term": self.term,
                    "lastLogIndex": self.last_index,
                    "lastLogTerm": self.last_term,
                }
            )

        return msgs


class VoteResponse(RPC):
    "Response to request for vote"

    def __init__(self, src, dests, term, vote_granted):
        RPC.__init__(self, src, dests)

        # The node's current term, so the candidate can update itself if needed
        self.term = term
        self.vote_granted = vote_granted

    def serialize(self):
        assert len(self.dests) == 1

        return [
            {
                "type": "voteResponse",
                "source": self.src,
                "destination": self.dests[0],
                "term": self.term,
                "voteGranted": self.vote_granted,
            }
        ]


class AppendEntries(RPC):
    "Invoked by leader to replicate log entries, and also as a heartbeat"

    def __init__(
        self,
        src,
        dests,
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit,
    ):
        """
        term :: int
            The leader's term
        prev_log_index :: int
            Index of the log entry immediately preceding new ones
        prev_log_term
            Term of prev_log_index entry
        entries :: [LogEntry]
            Log entries to store (empty if heartbeat)
        leader_commit :: int
            The leader's commit index
        """
        RPC.__init__(self, src, dests)

        self.term = term
        self.leader_id = leader_id
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries
        self.leader_commit = leader_commit

    def serialize(self):
        return [
            {
                "type": "appendEntries",
                "source": self.src,
                "destination": self.dests[0],
                "term": self.term,
                "prevLogIndex": self.prev_log_index,
                "prevLogTerm": self.prev_log_term,
                "entries": self.entries,
                "leaderCommit": self.leader_commit,
            }
        ]


class AppendResponse(RPC):
    "Response to append entries RPC"

    def __init__(self, src, dests, term, success, match_index):
        """
        term :: int
            Current term, in case the leader is out of date
        success :: bool
            true if the follower contained an entry matching prev
        """
        RPC.__init__(self, src, dests)

        self.term = term
        self.success = success
        self.match_index = match_index

    def serialize(self):
        return [
            {
                "type": "appendResponse",
                "source": self.src,
                "destination": self.dests[0],
                "term": self.term,
                "success": self.success,
                "matchIndex": self.match_index,
            }
        ]
