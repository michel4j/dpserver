import logging
import shutil
import threading
import os
import time
import pwd
import zmq
import json
import subprocess
from collections import deque
from multiprocessing import Process, cpu_count, Queue
from pathlib import Path
from szrpc.server import Server, Service, ResponseType
from szrpc import log


logger = log.get_module_logger('dpserver')
from .diffsig import signal_worker

SAVE_DELAY = .05  # Amount of time to wait for file to be written.

class Impersonator(object):
    """
    Context manager for temporarily switching the effective user
    """
    def __init__(self, user_name=None):
        self.user_name = user_name
        self.userdb = None
        if self.user_name is not None:
            self.userdb = pwd.getpwnam(user_name)

        self.gid = os.getgid()
        self.uid = os.getuid()

    def __enter__(self):
        if self.userdb is not None:
            try:
                os.setegid(self.userdb.pw_gid)
                os.seteuid(self.userdb.pw_uid)
            except OSError:
                logger.warning('Unable to impersonate user')

    def __exit__(self, *args):
        if self.userdb is not None:
            try:
                os.setegid(self.gid)
                os.seteuid(self.uid)
            except OSError:
                logger.warning('Unable to release impersonation')


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

        proc = subprocess.run(self.args, capture_output=True, start_new_session=True, user=user_name)

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
            start_new_session=True
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

    def __init__(self, num_workers=4):
        super().__init__()
        self.inbox = Queue(maxsize=5000)
        self.outbox = Queue(maxsize=5000)
        self.num_workers = num_workers
        self.signal_requests = {}
        self.signal_counts = {}
        self.signal_workers()

    def signal_workers(self):
        for i in range(self.num_workers):
            p = Process(target=signal_worker, args=(self.inbox, self.outbox))
            p.start()
        thread = threading.Thread(target=self.signal_monitor, daemon=True)
        thread.start()

    def signal_monitor(self):
        while True:
            request_id, output = self.outbox.get()
            self.signal_requests[request_id].reply(response_type=ResponseType.UPDATE, content=output)
            self.signal_counts[request_id] -= 1
            if self.signal_counts[request_id] <= 0:
                del self.signal_counts[request_id]
                del self.signal_requests[request_id]
            time.sleep(0.001)

    def signal_workload(self, request):
        directory = Path(request.kwargs['directory'])
        num_frames = request.kwargs['num_frames']
        timeout = request.kwargs.get('timeout', 360)
        start_time = time.time()
        if request.kwargs['type'] == 'file':
            template = request.kwargs['template']
            first_frame = request.kwargs['first']
            frames = deque(maxlen=num_frames)
            for i in range(num_frames):
                frames.append(directory.joinpath(template.format(i + first_frame)))

            frame = None
            while len(frames) and time.time() - start_time < timeout:
                if frame is None:
                    frame = frames.popleft()

                # Avoid reading a file while it's being written
                if frame.exists() and time.time() - frame.stat().st_mtime > SAVE_DELAY:
                    self.inbox.put([
                        request.request_id, 'file', str(frame)
                    ])
                    frame = None

                time.sleep(0.001)
        elif request.kwargs['type'] == 'stream':
            address = request.kwargs['address']
            context = zmq.Context()
            socket = context.socket(zmq.SUB)
            socket.connect(address)
            socket.setsockopt_string(zmq.SUBSCRIBE, "")

            header_data = []
            count = 0
            while count <= num_frames and time.time() - start_time < timeout:
                data = socket.recv_multipart()
                info = json.loads(data[0])
                if info['htype'] == 'dheader-1.0':
                    header_data = data
                elif info['htype'] == 'dimage-1.0' and header_data:
                    self.inbox.put((request.request_id, 'stream', header_data + data))
                    count += 1
                elif info['htype'] == 'dseries_end-1.0':
                    header_data = []
                time.sleep(0.001)
        return  time.time() - start_time < timeout

    def remote__signal_strength(self, request, **kwargs):
        """
        Perform signal strength analysis on a series of frames and return results piecemeal
        :param request: request object
        :param kwargs: keyworded arguments

        """
        self.signal_requests[request.request_id] = request
        self.signal_counts[request.request_id] = kwargs['num_frames']
        success = self.signal_workload(request)

        # wait for results
        if success:
            while request.request_id in self.signal_requests:
                time.sleep(0.001)
        else:
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

        cmd = Command('auto.process', directory=kwargs['directory'], args=args, outfile='report.json', outfmt=OutputFormat.JSON)
        for messages in cmd.run_async(kwargs['user_name']):
            request.reply(content=messages, response_type=ResponseType.UPDATE)

        output = cmd.output
        return output

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


def main():
    log.log_to_console()
    service = DPService()
    server = Server(service=service, port=9990)
    server.run()