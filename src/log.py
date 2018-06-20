"Log and related types"

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
        self.entries = []  # [LogEntry]

    def term(self, ind=None):
        """
        A safe accessor for indexing into the log.
        The interface is 0-indexed, unlike Ongaro's example.
        """

        if ind is None:
            if self.entries:
                return self.entries[-1].term
            return None

        if ind < 0 or ind >= len(self.entries):
            return None
        return self.entries[ind].term

    def update_until(self, index, entry):
        "Overwrite the last commited entry"

        # Note that slices exclude the upper index
        # Since we checked for equality at that index, we exclude it here
        self.entries = self.entries[0:index] + [entry]

    def append_entries(self, entries, prev_log_index):
        pass

    def __len__(self):
        return len(self.entries)


class LogEntry(object):
    "An entry in a node's log"

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
        return "LogEntry({},{})".format(self.term, self.entry)
