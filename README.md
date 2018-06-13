# raft

This is a key-value store implemented on top of the [Raft consensus protocol](https://raft.github.io/raft.pdf). I recently worked on an implementation of Chord, and curious and wanted to try this out.

It relies on the `chistributed` framework to act as a broker and simulate the network.

## Setup

Install requirements from `requirements.txt` (possibly in a virtual environment):

```
pip3 install -r requirements.txt
```

Runnable (human-readable) scripts are in `scripts`.
