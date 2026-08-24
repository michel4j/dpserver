import argparse
import signal
import sys
import logging
import re
import pandas as pd

from szrpc import log


def required_length(nmin, nmax):
    class RequiredLength(argparse.Action):
        def __call__(self, parser, args, values, option_string=None):
            if not nmin <= len(values) <= nmax:
                msg = f'argument "{self.dest}" requires between {nmin} and {nmax} arguments'
                raise argparse.ArgumentTypeError(msg)
            setattr(args, self.dest, values)
    return RequiredLength


def server_main():
    from dpserver import run_server, valid_cluster
    parser = argparse.ArgumentParser(description='Data Processing Server')
    parser.add_argument('-v',  action='store_true', help='Verbose Logging')
    parser.add_argument('-p', '--ports',  type=int, nargs="+", default=(9990, 9991), action=required_length(2, 3), help='Ports')
    parser.add_argument('--cluster', type=valid_cluster, help='Cluster parameters: partition:user@host,nodes,cores')
    parser.add_argument('--user', type=str, help='username for submitting jobs')

    args = parser.parse_args()
    if args.v:
        log.log_to_console(logging.DEBUG)
    else:
        log.log_to_console(logging.INFO)

    sys.exit(
        run_server(
            ports=tuple(args.ports),
            cluster=args.cluster,
            user=args.user
        )
    )


def signal_main():
    from dpserver import diffsig
    from mxio import DataSet
    parser = argparse.ArgumentParser(description='Signal Strength')
    parser.add_argument('-v', action='store_true', help='Verbose Logging')
    parser.add_argument('image', metavar='image', type=str, help='Images')

    args = parser.parse_args()
    if args.v:
        log.log_to_console(logging.DEBUG)
    else:
        log.log_to_console(logging.INFO)

    dset = DataSet.new_from_file(args.image)
    results = [
        diffsig.file_signal(image, dset.index)
        for image in dset.frames()
    ]
    df = pd.DataFrame.from_records(results)
    print(df.to_markdown())


def worker_main():
    from dpserver import run_worker, valid_cluster, valid_methods

    parser = argparse.ArgumentParser(description='Data Processing Worker')
    parser.add_argument('-v', action='store_true', help='Verbose Logging')
    parser.add_argument('-b', '--backend', type=str, help='Backend Address')
    parser.add_argument('-s', '--signal-threads', type=int, default=32, help='Number of Signal threads per worker')
    parser.add_argument('-n', '--instances', type=int, default=1, help='Number of Worker instances')
    parser.add_argument('--cluster', type=valid_cluster, help='Cluster parameters: partition:user@host,nodes,cores')
    parser.add_argument('--user', type=str, help='username for submitting jobs')
    parser.add_argument('--methods', type=valid_methods, help='Optional Comma Separated list of method names')

    args = parser.parse_args()
    if args.v:
        log.log_to_console(logging.DEBUG)
    else:
        log.log_to_console(logging.INFO)

    sys.exit(
        run_worker(
            signal_threads=args.signal_threads,
            backend=args.backend,
            instances=args.instances,
            cluster=args.cluster,
            user=args.user,
            methods=args.methods
        )
    )
