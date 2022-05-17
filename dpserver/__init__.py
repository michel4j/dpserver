import json
import os
import pwd
import re
import shutil
import subprocess

import time
import glob

from multiprocessing import Process, Queue
from pathlib import Path

import zmq
from szrpc import log
from szrpc.server import Server, WorkerManager, Service

logger = log.get_module_logger('dpserver')

from .diffsig import signal_worker, distl_worker

SAVE_DELAY = .1  # Amount of time to wait for file to be written.


class Impersonator(object):
    def __init__(self, user_name):
        self.user_name = user_name
        self.userdb = pwd.getpwnam(user_name)
        self.dgid = os.getgid()
        self.duid = os.getuid()
        self.gid = self.userdb.pw_gid
        self.uid = self.userdb.pw_uid

    def __enter__(self):
        os.setegid(self.gid)
        os.seteuid(self.uid)

    def __exit__(self, *args):
        os.setegid(self.dgid)
        os.seteuid(self.duid)


class OutputFormat:
    RAW = 0
    JSON = 1


class Command(object):
    def __init__(self, command, directory=None, args=(), outfile=None, outfmt=OutputFormat.RAW):
        self.args = [shutil.which(command), *args]
        self.directory = directory if directory is None else Path(directory)
        self.outfile = outfile if not all((outfile, directory)) else self.directory.joinpath(outfile)
        self.outfmt = outfmt
        self.stdout = b''
        self.stderr = b''
        self.output = None
        self.retcode = 0

    def run(self, user_name=None):
        if self.directory and self.directory.exists():
            os.chdir(self.directory)
        proc = subprocess.run(self.args, capture_output=True, start_new_session=True, user=user_name, group=user_name)

        self.stdout = proc.stdout
        self.stderr = proc.stderr
        if proc.returncode == 0 and self.outfile is not None:
            with open(self.outfile, 'rb') as fobj:
                if self.outfmt == OutputFormat.JSON:
                    self.output = json.load(fobj)
                else:
                    self.output = fobj.read()
        self.retcode = proc.returncode
        return proc.returncode == 0

    def run_async(self, user_name, output='stderr'):
        """
        Run the command asynchronously and return the output from the output stream
        :param user_name: Run the command as the user specified by user-name
        :param output: 'stderr' or 'stdout'
        :return: yields output as command is running. Iterate through the method to get all output
        """
        proc = subprocess.Popen(
            self.args, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True, user=user_name,
            group=user_name, start_new_session=True, shell=True
        )
        stream = getattr(proc, output)
        for stdout_line in iter(stream.readline, ""):
            self.stdout += stdout_line.encode('utf-8')
            yield stdout_line.rstrip()
        return_code = proc.wait()
        self.retcode = return_code

        if return_code == 0 and self.outfile is not None:
            with open(self.outfile, 'rb') as fobj:
                if self.outfmt == OutputFormat.JSON:
                    self.output = json.load(fobj)
                else:
                    self.output = fobj.read()
        elif return_code:
            raise subprocess.CalledProcessError(return_code, self.args)


class DPService(Service):

    def __init__(self, signal_threads=4, method=signal_worker):
        super().__init__()
        self.method = method
        self.num_workers = signal_threads

    def start_signal_workers(self, inbox, outbox):
        signal_workers = []
        for i in range(self.num_workers):
            p = Process(target=self.method, args=(inbox, outbox))
            p.start()
            signal_workers.append(p)
        return signal_workers

    def remote__signal_strength(self, request, **kwargs):
        """
        Perform signal strength analysis on a series of frames and return results piecemeal
        :param request: request object
        :param kwargs: keyworded arguments
        """

        inbox = Queue()
        outbox = Queue()

        num_tasks = 0  # number of outstanding tasks submitted
        timeout = kwargs.get('timeout', 30)  # maximum time to wait for last result
        signal_workers = self.start_signal_workers(inbox, outbox)

        if request.kwargs['type'] == 'file':
            directory = Path(request.kwargs['directory'])
            num_frames = request.kwargs['num_frames']
            template = request.kwargs['template']
            first_frame = request.kwargs['first']

            if num_frames == 0:
                raise RuntimeError('Zero frames requested!')

            wildcard = str(directory.joinpath(re.sub(r'{[^{]+}', '*', template)))
            frames = (
                (str(directory.joinpath(template.format(i + first_frame))), i + first_frame)
                for i in range(num_frames)
            )
            last_frame = time.time()
            started = False
            frames_remain = True
            cur_frame = None
            index = 1

            while True:
                # Fetch pending results and sent to broker.
                if not outbox.empty():
                    output = outbox.get()
                    request.reply(output)
                    num_tasks -= 1

                # Find files from requested set and add them to the queue if they exist on disk
                # check directory listing for files. Needed instead of simple os.path.exists because without
                # updating the directory listing, path.exists sometimes returns false on some NFS partitions
                on_disk = set(glob.glob(wildcard))
                if cur_frame is None and frames_remain:
                    try:
                        cur_frame, index = next(frames)
                        last_frame = time.time()
                    except StopIteration:
                        frames_remain = False
                elif cur_frame and cur_frame in on_disk:
                    if os.path.exists(cur_frame):
                        inbox.put(('file', (cur_frame, index)))
                        started = True
                        num_tasks += 1
                    cur_frame = None

                all_frames_fetched = (started and not frames_remain and cur_frame is None)
                frames_timed_out = (cur_frame and time.time() - last_frame > timeout)

                # break out of loop if no pending tasks, no pending frames or next frame timed-out
                if num_tasks == 0 and (all_frames_fetched or frames_timed_out):
                    break

                time.sleep(0.05)

        elif request.kwargs['type'] == 'stream':
            address = request.kwargs['address']
            context = zmq.Context()
            socket = context.socket(zmq.SUB)
            socket.connect(address)
            socket.setsockopt_string(zmq.SUBSCRIBE, "")

            header_data = []
            all_frames_fetched = False
            last_frame = time.time()

            while True:
                # Fetch pending results and sent to broker.
                if not outbox.empty():
                    output = outbox.get()
                    request.reply(output)
                    num_tasks -= 1

                # Fetch frames from stream and submit to signal workers
                if socket.poll(10, zmq.POLLIN):
                    data = socket.recv_multipart()
                    info = json.loads(data[0])
                    if info['htype'] == 'dheader-1.0':
                        header_data = data
                    elif info['htype'] == 'dimage-1.0' and header_data:
                        inbox.put(('stream', header_data + data))
                        last_frame = time.time()
                        num_tasks += 1
                    elif info['htype'] == 'dseries_end-1.0':
                        all_frames_fetched = True

                # break out of loop if no outstanding results, no pending frames or next frame timed-out
                if num_tasks == 0 and (all_frames_fetched or time.time() - last_frame > timeout):
                    break

            socket.close()
            context.term()

        inbox.put('END')
        for proc in signal_workers:
            proc.join()

        if not num_tasks == 0:
            msg = 'Signal strength timed-out!'
            logger.error(msg)
            raise RuntimeError(msg)

    def remote__process_mx(self, request, **kwargs):
        """
        Process an MX dataset

        :param request: request object
        :param kwargs: keyworded arguments
        """

        args = [
            '--dir={}'.format(kwargs['directory'])
        ]
        args += ['--screen'] if kwargs.get('screen') else []
        args += ['--anom'] if kwargs.get('anomalous') else []
        args += ['--mad'] if kwargs.get('mad') else []
        args += kwargs['file_names']

        cmd = Command('auto.process', directory=kwargs['directory'], args=args, outfile='report.json',
                      outfmt=OutputFormat.JSON)
        success = cmd.run(user_name=kwargs['user_name'])
        # for messages in cmd.run_async(user_name=kwargs['user_name']):
        #    request.reply(content=messages, response_type=ResponseType.UPDATE)

        if success:
            return cmd.output
        else:
            err = cmd.stderr.decode('utf-8').splitlines()[-1]
            msg = f'AutoProcess failed with error #{cmd.retcode}: {err}'
            logger.error(err)
            raise RuntimeError(msg)

    def remote__process_xrd(self, request, **kwargs):
        """
        Process an XRD dataset

        :param directory: directory for output
        :param user_name: user name to run as
        :return: a dictionary of the report
        """

        args = []
        args += ['--calib'] if kwargs.get('calib') else []
        args += kwargs['file_names']
        cmd = Command('auto.powder', kwargs['directory'], args, outfile='report.json', outfmt=OutputFormat.JSON)
        success = cmd.run(kwargs['user_name'])
        if success:
            return cmd.output
        else:
            err = cmd.stderr.decode('utf-8').splitlines()[-1]
            msg = f'AutoProcess failed with error #{cmd.retcode}: {err}'
            logger.error(msg)
            raise RuntimeError(msg)

    def remote__process_misc(self, request, **kwargs):
        """
        Process an XRD dataset

        :param info: dictionary containing parameters
        :param directory: directory for output
        :param user_name: user name to run as
        :return: a dictionary of the report
        """

        args = [
            '-d',
            kwargs['directory'],
            kwargs['file_names'][0]
        ]
        cmd = Command('msg', kwargs['directory'], args, outfile='report.json', outfmt=OutputFormat.JSON)
        success = cmd.run(kwargs['user_name'])
        if success:
            return cmd.output
        else:
            err = cmd.stderr.decode('utf-8').splitlines()[-1]
            msg = f'MSG failed with error #{cmd.retcode}: {err}'
            logger.error(msg)
            raise RuntimeError(msg)


def run_server(ports, signal_threads, instances=1):
    service = DPService(signal_threads=signal_threads, method=distl_worker)
    server = Server(service=service, ports=ports, instances=instances)
    server.run()


def run_worker(signal_threads, backend, instances=1):
    service = DPService(signal_threads=signal_threads, method=distl_worker)
    server = WorkerManager(service=service, backend=backend, instances=instances)
    server.run()


PACKAGE_DIR = os.path.dirname(os.path.dirname(__file__))


def get_version(prefix='v', package=PACKAGE_DIR, name=None):
    # Return the version if it has been injected into the file by git-archive
    tag_re = re.compile(rf'\btag: {prefix}(\d[^,]*)\b')
    version = tag_re.search('$Format:%D$')
    name = __name__.split('.')[0] if not name else name

    if version:
        return version.group(1)

    package_dir = package

    if os.path.isdir(os.path.join(package_dir, '.git')):
        # Get the version using "git describe".
        version_cmd = 'git describe --tags --abbrev=0'
        release_cmd = 'git rev-list HEAD ^$(git describe --abbrev=0) | wc -l'
        try:
            version = subprocess.check_output(version_cmd, shell=True).decode().strip()
            release = subprocess.check_output(release_cmd, shell=True).decode().strip()
            return f'{version}.{release}'.strip(prefix)
        except subprocess.CalledProcessError:
            version = '0.0'
            release = 'dev'
            return f'{version}.{release}'.strip(prefix)
    else:
        try:
            from importlib import metadata
        except ImportError:
            # Running on pre-3.8 Python; use importlib-metadata package
            import importlib_metadata as metadata

        version = metadata.version(name)

    return version