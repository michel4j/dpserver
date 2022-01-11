
import os
import pwd
import json
import subprocess
from pathlib import Path
from szrpc.server import Server, Service
from szrpc import log


logger = log.get_module_logger('dpserver')


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
    def __init__(self, command, directory, args=(), outfile=None, outfmt=OutputFormat.RAW):
        self.args = [command, *args]
        self.directory = Path(directory)
        self.outfile = outfile
        self.outfmt = outfmt
        self.stdout = b''
        self.stderr = b''
        self.output = None
        self.retcode = 0

    def run(self, user_name=None):
        with Impersonator(user_name):
            if not self.directory.exists():
                self.directory.mkdir(parents=True, exist_ok=True)
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


class DPService(Service):

    def remote__process_mx(self, request, **kwargs):
        """
        Process an MX dataset

        :param info: dictionary containing parameters
        :param directory: directory for output
        :param user_name: user name to run as
        :return: dictionary containing the report
        """

        args = [
            '--dir={}'.format(kwargs['directory'])
        ]
        args += ['--screen'] if kwargs.get('screen') else []
        args += ['--anom'] if kwargs.get('anomalous') else []
        args += ['--mad'] if kwargs.get('mad') else []
        args += kwargs['file_names']
        cmd = Command('auto.process', kwargs['directory'], args, outfile='report.json', outfmt=OutputFormat.JSON)
        success = cmd.run(kwargs['user_name'])
        if success:
            return cmd.output
        else:
            err = cmd.stderr.decode('utf-8').splitlines()[-1]
            msg = f'AutoProcess failed with error #{cmd.retcode}: {err}'
            logger.error(msg)
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


def main():
    log.log_to_console()
    service = DPService()
    server = Server(service=service, port=9990)
    server.run()