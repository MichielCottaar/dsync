"""Collecting information from the database."""
from .models import Dataset, DataStore, ToSync


def datasets(session, *args, **kwargs):
    """Return a list of all datasets."""
    return _get_data(session, Dataset, *args, **kwargs)


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
