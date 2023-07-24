"""Collecting information from the database."""
from .models import DataStore, in_session


@in_session
def stores(session):
    """Return a list of all remote stores and their connections."""
    return [(store, store.get_connection()) for store in session.query(DataStore).all()]
