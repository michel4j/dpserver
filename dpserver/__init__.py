import json
import os
import pwd
import re
import shutil
import subprocess
import psutil
import resource
import time
import glob
import numpy

from multiprocessing import Process, Value, Manager
from multiprocessing.managers import SyncManager
from threading import Thread
from pathlib import Path

import zmq
from mxio.formats import cbf, eiger
from szrpc import log
from szrpc.server import Server, ServiceFactory, WorkerManager, Service

logger = log.get_module_logger('dpserver')

from .diffsig import signal_worker, distl_worker

SAVE_DELAY = .1  # Amount of time to wait for file to be written.
START_DELAY = 30

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


class ResultManager(Thread):
    def __init__(self, request, results):
        super().__init__(target=self.execute, daemon=True)
        self.results = results
        self.request = request
        self.num_items = 0
        self.count = 0
        self.stopped = False
        self.pending = True

    def outstanding(self):
        return self.pending or (self.num_items and self.num_items <= self.count)

    def stop(self):
        self.stopped = True

    def update_status(self, num_items):
        self.num_items = num_items
        self.pending = False

    def execute(self):
        self.count = 0
        self.stopped = False

        while self.outstanding() and not self.stopped:
            if not self.results.empty():
                output = self.results.get()
                self.request.reply(output)
                self.results.task_done()
                self.count += 1
            time.sleep(0.025)


class StreamMonitor(Process):
    def __init__(self, request, tasks):
        self.num_tasks = Value('i', 0)
        address = request.kwargs['address']
        timeout = request.kwargs['timeout']
        tasks = tasks
        name = request.kwargs['name']
        template = os.path.join('/dev/shm', f'{name}_{{:05d}}.cbf')
        super().__init__(target=self.execute, args=(address, name, template, tasks, timeout, self.num_tasks))

    @staticmethod
    def execute(address, name, template, tasks, timeout, num_tasks):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.connect(address)
        socket.setsockopt_string(zmq.SUBSCRIBE, "")
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)
        try:
            dataset = None
            end_time = time.time() + START_DELAY
            while time.time() < end_time:
                socks = dict(poller.poll())
                if socket in socks:
                    data = socket.recv_multipart()
                    info = json.loads(data[0])
                    if info['htype'] == 'dheader-1.0':
                        dataset = eiger.EigerStream()
                        dataset.read_header(data)
                        logger.debug(f'New raster: {name} ...')
                    elif info['htype'] == 'dimage-1.0' and dataset is not None:
                        dataset.read_image(data)
                        index = dataset.header['frame_number']
                        filename = template.format(index)
                        logger.debug(f'Saving CBF File:  {filename} ...')
                        cbf.write_minicbf(filename, dataset.header, dataset.data.astype(numpy.int32))
                        tasks.put(('file', (filename, index)))
                        with num_tasks.get_lock():
                            num_tasks.value += 1
                        end_time = time.time() + timeout
                    elif info['htype'] == 'dseries_end-1.0':
                        logger.debug(f'{name}: {num_tasks.value} frames saved!')
                        break
            else:
                logger.warning('{name}: timed out waiting for stream data!')

        finally:
            socket.close()
            context.term()


class FileMonitor(Process):
    def __init__(self, request, tasks):
        self.num_tasks = Value('i', 0)
        directory = Path(request.kwargs['directory'])
        template = request.kwargs['template']
        first_frame = request.kwargs['first']
        num_frames = request.kwargs['num_frames']
        timeout = request.kwargs['timeout']
        tasks = tasks
        super().__init__(
            target=self.execute, args=(tasks, directory, template, first_frame, num_frames, timeout, self.num_tasks)
        )

    @staticmethod
    def execute(tasks, directory, template, first_frame, num_frames, timeout, num_tasks):

        wildcard = str(directory.joinpath(re.sub(r'{[^{]+}', '*', template)))
        frames = (
            (str(directory.joinpath(template.format(i + first_frame))), i + first_frame)
            for i in range(num_frames)
        )

        started = False
        frames_remain = True
        cur_frame = None
        index = 1

        end_time = time.time() + START_DELAY
        while time.time() < end_time:
            # Fetch pending results and sent to broker.

            # Find files from requested set and add them to the queue if they exist on disk
            # check directory listing for files. Needed instead of simple os.path.exists because without
            # updating the directory listing, path.exists sometimes returns false on some NFS partitions
            on_disk = set(glob.glob(wildcard))
            if frames_remain:
                if cur_frame is None:
                    try:
                        cur_frame, index = next(frames)
                    except StopIteration:
                        frames_remain = False
                elif cur_frame in on_disk:
                    tasks.put(('file', (cur_frame, index)))
                    with num_tasks.get_lock():
                        num_tasks.value += 1
                    started = True
                    cur_frame = None
                    end_time = time.time() + timeout
            elif started and cur_frame is None:
                break
            time.sleep(0.01)
        else:
            logger.warning('{name}: timed out saving CBF dataset!')


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

    @staticmethod
    def nicer():
        pid = os.getpid()
        ps = psutil.Process(pid)
        ps.set_nice(10)
        resource.setrlimit(resource.RLIMIT_CPU, (1, 1))

    def run(self, user_name=None, nice=True):
        if self.directory and self.directory.exists():
            os.chdir(self.directory)
        nice_func = self.nicer if nice else None
        proc = subprocess.run(
            self.args, capture_output=True, start_new_session=True,
            user=user_name, group=user_name, preexec_func=nice_func
        )

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

    def run_async(self, user_name, output='stderr', nice=False):
        """
        Run the command asynchronously and return the output from the output stream
        :param user_name: Run the command as the user specified by user-name
        :param output: 'stderr' or 'stdout'
        :return: yields output as command is running. Iterate through the method to get all output
        """

        nice_func = self.nicer if nice else None
        proc = subprocess.Popen(
            self.args, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True, user=user_name,
            group=user_name, start_new_session=True, shell=True, preexec_fn=nice_func
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

    def start_signal_workers(self, tasks, results):
        signal_workers = []
        for i in range(self.num_workers):
            p = Process(target=self.method, args=(tasks, results))
            p.start()
            signal_workers.append(p)
        return signal_workers

    def remote__signal_strength(self, request, **kwargs):
        """
        Perform signal strength analysis on a series of frames and return results piecemeal
        :param request: request object
        :param kwargs: keyworded arguments
        """

        manager = Manager()
        tasks = manager.Queue()
        results = manager.Queue()

        timeout = kwargs.get('timeout', 30)  # maximum time to wait for last result
        signal_workers = self.start_signal_workers(tasks, results)

        if request.kwargs['num_frames'] > 0 and request.kwargs['type'] in ['file', 'stream']:
            result_manager = ResultManager(request, results)
            task_manager = {
                'file': FileMonitor,
                'stream': StreamMonitor
            }[request.kwargs['type']](request, tasks)

            logger.info(f"Monitoring {request.kwargs['type']} signal-strength ...")
            task_manager.start()
            result_manager.start()

            # Wait for all tasks to be submitted
            end_time = time.time() + START_DELAY
            last_count = task_manager.num_tasks.value
            while time.time() < end_time:
                if not task_manager.is_alive():
                    break

                if task_manager.num_tasks.value > last_count:
                    end_time = time.time() + timeout

                time.sleep(0.01)
            else:
                task_manager.terminate()
                logger.debug(f'Task Manager timed out after {task_manager.num_tasks.value} tasks!')

            # Wait for all results to be returned
            tasks.join()
            result_manager.update_status(num_items=task_manager.num_tasks.value)
            for _ in signal_workers:
                tasks.put('STOP')

            end_time = time.time() + START_DELAY
            while time.time() < end_time:
                if not result_manager.is_alive():
                    break
                time.sleep(0.1)
            else:
                result_manager.stop()
                logger.debug('Result Manager timed out!')

        for i, proc in enumerate(signal_workers):
            if proc.is_alive():
                logger.debug(f'Terminating signal evaluator #{i} ...')
                proc.terminate()

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
        success = cmd.run(user_name=kwargs['user_name'], nice=True)

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
        success = cmd.run(kwargs['user_name'], nice=True)
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
        success = cmd.run(kwargs['user_name'], nice=True)
        if success:
            return cmd.output
        else:
            err = cmd.stderr.decode('utf-8').splitlines()[-1]
            msg = f'MSG failed with error #{cmd.retcode}: {err}'
            logger.error(msg)
            raise RuntimeError(msg)


def run_server(ports, signal_threads, instances=1):
    factory = ServiceFactory(DPService, signal_threads=signal_threads, method=distl_worker)
    server = Server(factory, ports=ports, instances=instances)
    server.run(balancing=False)


def run_worker(signal_threads, backend, instances=1):
    factory = ServiceFactory(DPService, signal_threads=signal_threads, method=distl_worker)
    server = WorkerManager(factory, address=backend, instances=instances)
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