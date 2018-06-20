"Responses that go the client"

from message import Message

# pylint: disable=too-few-public-methods
# pylint: disable=missing-docstring


class SetResponse(Message):
    "Response to client set request"

    def __init__(self, src, dests):
        Message.__init__(self, dests)

    def serialize(self):
        pass


class GetResponse(Message):
    "Response to client get request"

    def __init__(self, src, dests):
        Message.__init__(self, dests)

    def serialize(self):
        pass
