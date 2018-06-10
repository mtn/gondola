"A node log entry"

# pylint: disable=too-few-public-methods


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
        return "LogEntry"
