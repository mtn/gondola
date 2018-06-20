"Responses that go the client (and thus have no dest field)"

from message import Message

# pylint: disable=too-few-public-methods
# pylint: disable=too-many-arguments
# pylint: disable=missing-docstring


class ClientResponse(Message):
    def __init__(self, src, req_id):
        Message.__init__(self, src, None)

        # The id that identifies the request itself
        self.req_id = req_id


class SetResponse(ClientResponse):
    "Response to client set request"

    def __init__(self, src, req_id, key=None, value=None, error=None):
        assert key is not None and value is not None or error is not None

        ClientResponse.__init__(self, src, req_id)

        self.key = key
        self.val = value
        self.err = error

    def serialize(self):
        serialized = {"source": self.src, "type": "getResponse", "id": self.req_id}

        if self.val:
            serialized["value"] = self.val
            serialized["key"] = self.key
        else:
            serialized["error"] = self.err

        return [serialized]

    def __repr__(self):
        return "SET({})".format(self.req_id)


class GetResponse(ClientResponse):
    "Response to client get request"

    def __init__(self, src, req_id, key=None, value=None, error=None):
        assert key is not None and value is not None or error is not None

        ClientResponse.__init__(self, src, req_id)

        self.key = key
        self.val = value
        self.err = error

    def serialize(self):
        serialized = {"source": self.src, "type": "getResponse", "id": self.req_id}

        if self.err:
            serialized["error"] = self.err
        else:
            serialized["key"] = self.key
            serialized["value"] = self.val

        return [serialized]

    def __repr__(self):
        return "GET({})".format(self.req_id)
