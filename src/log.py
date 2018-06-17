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


class Log(object):

    def __init__(self):
        self.log = []  # [LogEntry]

    def term(self, ind=None):
        """
        A safe accessor for indexing into the log.
        The interface is 0-indexed, unlike Ongaro's example.
        """

        if ind is None:
            return self.log[-1].term

        if ind < 0 or ind >= len(self.log):
            return 0
        return self.log[ind].term

    def __len__(self):
        return len(self.log)


class LogEntry(object):
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
