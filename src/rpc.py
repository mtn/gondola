"RPC objects to simplify message passing"

# pylint: disable=too-few-public-methods
# pylint: disable=too-many-arguments
# pylint: disable=missing-docstring


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
                    "lastTerm": self.last_term,
                }
            )

        return msgs


class VoteResponse(RPC):
    "Response to request for vote"

    def __init__(self, src, dests, term, vote_granted):
        RPC.__init__(self, src, dests)

        # The node's current term, so the candidate can update itself
        self.term = term
        self.vote_granted = vote_granted

    def serialize(self):
        assert len(self.dests) == 1

        return [
            {
                "source": self.src,
                "destination": self.dests[0],
                "term": self.term,
                "voteGranted": self.vote_granted,
            }
        ]


class AppendEntries(RPC):
    "Invoked by leader to replicate log entries, and also as a heartbeat"

    def __init__(self, src, dests):
        RPC.__init__(self, src, dests)
        # TODO


class AppendResponse(RPC):
    "Response to append entries RPC"

    def __init__(self, src, dests):
        RPC.__init__(self, src, dests)
        # TODO
