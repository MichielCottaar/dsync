"""Collecting information from the database."""
from .models import Dataset, DataStore, ToSync


def stores(session):
    """Return a list of all remote stores and their connections."""
    return [(store, store.get_connection()) for store in session.query(DataStore).all()]


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
