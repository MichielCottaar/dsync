"""Defines how to transver data to/from a data store."""
import os.path as op
import random
import string
from abc import ABC, abstractmethod
from functools import lru_cache
from subprocess import PIPE, Popen, run

import rich


def get_transfer_protocol(data_store):
    """
    Get transfer protocol corresponding to data store.

    The result is cached to stop duplicating connections to the same machine.
    """
    return _get_transfer_protocol(data_store.name, data_store.link, data_store.type)


@lru_cache
def _get_transfer_protocol(name, link, type):
    if type == "ssh":
        return SSHTransfer(name, link)
    elif type == "disc":
        return DiscTransfer(name, link)


class TransferProtocol(ABC):
    """Interface to transfer data to/from a DataStore."""

    _setup_suceeded = None

    def __init__(self, name, link):
        """Store the link to the DataStore."""
        self.name = name
        self.link = link

    def setup_connection(self, verbose=True):
        """
        Create a new connection.

        Returns True if the connection was successfully established.
        If run multiple times, this will only do any work the first time.
        """
        if self._setup_suceeded is None:
            if verbose:
                rich.print(f"Attempting to connect with {self.name}.")
            self._setup_suceeded = self._setup_connection()
            if verbose:
                if self._setup_suceeded:
                    rich.print(f"Succesfully connected with {self.name}.")
                else:
                    rich.print(f"Failed to connect with {self.name}.")
        return self._setup_suceeded

    @abstractmethod
    def _setup_connection(
        self,
    ):
        """Set up the connection in a sub-class."""
        pass

    def local_path(self, dataset_name, relative_path=""):
        """Return local path to `dataset_name`."""
        return op.expanduser(f"~/Work/data/{dataset_name}/{relative_path}")

    @abstractmethod
    def remote_path(self, dataset_name, relative_path=""):
        """Return path to `dataset_name` in the DataStore."""
        pass

    def rsync_command(self, dataset_name, relative_path="", from_local=True):
        """
        Return the rsync command needed to sync the data.

        By default data will be synced from the local machine to the DataStore.
        Set `from_local` to False to revert this.
        """
        cmd = ["rsync", "-aP", "--delete", "--bwlimit=256000"]
        paths = [
            self.local_path(dataset_name, relative_path),
            self.remote_path(dataset_name, relative_path),
        ]
        if from_local:
            res = cmd + paths
        else:
            res = cmd + paths[::-1]
        rich.print("running", " ".join(res))
        return res

    def sync(self, dataset_name, relative_path="", from_local=True):
        """
        Sync the dataset from/to the data store.

        By default data will be synced from the local machine to the DataStore.
        Set `from_local` to False to revert this.
        """
        cmd = self.rsync_command(dataset_name, relative_path, from_local=from_local)
        return run(cmd).returncode


class DiscTransfer(TransferProtocol):
    """
    Transfer data to/from a remote disc.

    Typically used for archiving.
    """

    def _setup_connection(self):
        return op.isdir(self.remote_path("."))

    def remote_path(self, dataset_name, relative_path=""):
        """Return path to dataset on external disc."""
        return f"/Volumes/{self.link}/data-archive/{dataset_name}/{relative_path}"


class SSHTransfer(TransferProtocol):
    """
    Transfer data to/from a remote contactable by SSH.

    Presumes that the SSH will set up a reverse tunnel to SSH back to the computer called "mac".
    """

    @staticmethod
    def _random_string(length=20):
        return "".join(random.choice(string.ascii_letters) for _ in range(length))

    def _setup_connection(self):
        for link in self.link.split(","):
            host = link.strip()
            connection = Popen(
                ["ssh", host],
                shell=False,
                stdin=PIPE,
                stdout=PIPE,
                stderr=PIPE,
                universal_newlines=True,
                bufsize=0,
            )
            pwd = self._random_string() + "\n"
            connection.stdin.write("echo " + pwd)
            connection.stdin.flush()
            for line in connection.stdout:
                if line == pwd:
                    self.connection = connection
                    return True
        return False

    def local_path(self, dataset_name, relative_path=""):
        """Return path to dataset on local laptop."""
        return "mac:" + super().local_path(dataset_name, relative_path)

    def remote_path(self, dataset_name, relative_path=""):
        """Return path to dataset on ssh server."""
        return op.join(op.join("Work", "data", dataset_name) + "/", relative_path)

    def sync(self, dataset_name, relative_path="", from_local=True):
        """
        Sync the dataset from/to the SSH server.

        By default data will be synced from the local machine to the SSH server.
        Set `from_local` to False to revert this.
        """
        cmd = self.rsync_command(dataset_name, relative_path, from_local=from_local)
        cmd_fixed = " ".join([part.replace(" ", "\\ ") for part in cmd])
        self.connection.stdin.write(cmd_fixed + "\n")
        pwd = self._random_string() + "\n"
        self.connection.stdin.write("echo " + pwd)
        self.connection.stdin.flush()

        for line in self.connection.stdout:
            if "file list" in line or "files to consider" in line:
                print(line, end="")
            if line == pwd:
                return 0
        return 1
