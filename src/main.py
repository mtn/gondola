"Entry point, called by chistributed"

import argparse
from node import Node

# pylint: disable=missing-docstring
# pylint: disable=invalid-name

def parse_args():
    parser = argparse.ArgumentParser(description="node script for chidistributed")
    parser.add_argument("--pub-endpoint")
    parser.add_argument("--router-endpoint")
    parser.add_argument("--node-name")
    parser.add_argument("--peer", action="append")
    parser.add_argument("--debug", action="store_true")

    return parser.parse_args()

if __name__ == "__main__":
    in_args = parse_args()

    n = Node(
        in_args.node_name,
        in_args.pub_endpoint,
        in_args.router_endpoint,
        in_args.peer,
        in_args.debug,
    )
    n.run()
