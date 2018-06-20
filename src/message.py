"Base message type"

# pylint: disable=too-few-public-methods
# pylint: disable=missing-docstring


class Message(object):
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
