"Ledger and related types"

# pylint: disable=too-few-public-methods
# pylint: disable=missing-docstring


class Request(object):
    pass

class SetRequest(Request):
    def __init__(self, key, val):
        self.key = key
        self.val = val

    def __repr__(self):
        return "SET {}: {}".format(self.key, self.val)

class GetRequest(Request):
    def __init__(self, key):
        self.key = key

    def __repr__(self):
        return "GET {}".format(self.key)

class LedgerEntry(object):
    "An entry in a node's ledger"

    def __init__(self, term, entry):
        """
        term :: int
            The term the entry was added in
        entry :: Request
            A request (set or get)
        """
        self.term = term
        self.entry = entry

    def __repr__(self):
        return "LedgerEntry({},{})".format(self.term, self.entry)
