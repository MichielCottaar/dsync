"""Collecting information from the database."""
import os.path as op

import rich

from .models import Dataset, DataStore, ToSync, in_session


def datasets(session, *args, **kwargs):
    """Return a list of all datasets."""
    return _get_data(session, Dataset, *args, **kwargs)


def get_dataset(session, name=None, current_directory=None):
    """
    Return a specific dataset identified by name.

    If `name` is None, find return the dataset corresponding to the current directory instead.
    If the current directory is not in a dataset or
    if `name` does not match an existing dataset, returns None.
    """
    if name is None:
        if current_directory is None:
            current_directory = op.curdir
        abs_dir = op.normpath(op.abspath(current_directory)).split(op.sep)
        data_dir = op.expanduser("~/Work/data").split(op.sep)
        if abs_dir[: len(data_dir)] == data_dir:
            name = abs_dir[len(data_dir)]
            rich.print(f"Current dataset is determined to be {name}")
        else:
            return None
    return session.query(Dataset).get(name)


def stores(session, *args, **kwargs):
    """Return a list of all remote stores."""
    return _get_data(session, DataStore, *args, **kwargs)


def _get_data(session, cls, name=None, as_list=False):
    """Query the tables for datasets or data stores."""
    if name is not None:
        result = session.query(cls).get(name)
        if result is None:
            raise ValueError(f"Attempted to get non-existant {cls.__name__}: {name}.")
        if as_list:
            return [result]
        return result
    return session.query(cls).all()


def last_sync(dataset, data_store, session):
    """Find the datetime of the last sync (None if never synced or not syncing anymore)."""
    if isinstance(dataset, Dataset):
        dataset = dataset.name
    if isinstance(data_store, DataStore):
        data_store = data_store.name
    to_sync = session.query(ToSync).get((dataset, data_store))
    return (
        None
        if to_sync is None
        else ("upcoming" if to_sync.last_sync is None else to_sync.last_sync)
    )


@in_session
def complete_datasets(ctx, param, incomplete, session, archived=False):
    """Provide shell completion for datasets."""
    all_names = [d.name for d in datasets(session) if archived == d.archived]
    return [n for n in all_names if n.lower().startswith(incomplete.lower())]


@in_session
def complete_stores(ctx, param, incomplete, session, only_remotes=False):
    """Provide shell completion for data stores."""
    all_names = [s.name for s in stores(session) if not (only_remotes and s.is_archive)]
    return [n for n in all_names if n.lower().startswith(incomplete.lower())]
