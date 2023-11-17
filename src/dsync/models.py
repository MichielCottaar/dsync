"""Define dsync database models."""
import os
import os.path as op
from datetime import datetime
from functools import lru_cache, wraps

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    String,
    create_engine,
)
from sqlalchemy.orm import Session, declarative_base, relationship

from .transfer import get_transfer_protocol

Base = declarative_base()


class DataStore(Base):
    """Data storage location (ssh/disc)."""

    __tablename__ = "data_store"

    name = Column(String, primary_key=True)
    link = Column(String)
    type = Column(String)  # ssh or disc
    is_archive = Column(Boolean)
    syncs = relationship(
        "ToSync",
        back_populates="store",
        cascade="all",
    )

    def __repr__(
        self,
    ):
        """Represent data store as string."""
        return f"DataStore(name={self.name})"

    def get_connection(
        self,
    ):
        """
        Get the best connection to the DataStore.

        Returns None if no connection is available.
        """
        transfer = get_transfer_protocol(self)
        return transfer if transfer.setup_connection() else None


class Dataset(Base):
    """Dataset to be synced across data stores."""

    __tablename__ = "dataset"

    name = Column(String, primary_key=True)
    description = Column(String)
    archived = Column(Boolean, default=False)
    primary_name = Column(
        String, ForeignKey("data_store.name"), nullable=True, default=None
    )
    primary = relationship("DataStore")
    syncs = relationship(
        "ToSync",
        back_populates="dataset",
        cascade="all",
    )
    latest_edit = Column(DateTime, nullable=True)

    @property
    def local_path(
        self,
    ):
        """
        Path to local version of dataset.

        The data should exist in this location unless `self.archive` is True.
        """
        return op.join(op.expanduser("~/Work/data"), self.name) + "/"

    def update_latest_edit(
        self,
    ):
        """Update time of latest edit, presuming the local version is up to date."""
        max_mtime = 0
        for dirname, _, files in os.walk(self.local_path):
            for fname in files:
                full_path = os.path.join(dirname, fname)
                mtime = os.stat(full_path).st_mtime
                if mtime > max_mtime:
                    max_mtime = mtime
        self.latest_edit = datetime.fromtimestamp(max_mtime)

    def all_syncs(self, session):
        """Return dictionary with all sync objects related with this DataSet."""
        existing_syncs = {tosync.store.name: tosync for tosync in self.syncs}
        for store in session.query(DataStore).all():
            if store.name not in existing_syncs and store.is_archive:
                new_sync = ToSync(dataset=self, store=store)
                session.add(new_sync)
                existing_syncs[store.name] = new_sync
        return existing_syncs

    def __repr__(
        self,
    ):
        """Represent dataset as string."""
        return f"Dataset(name={self.name})"

    def sync(self, session, store=None):
        """Sync this dataset with the given store links."""
        if self.primary is not None and store is None:
            primary_sync = session.query(ToSync).get((self.name, self.primary_name))
            if primary_sync.sync() != 0:
                return 1
        all_syncs = self.all_syncs(session)
        if store is not None:
            if store not in all_syncs:
                return 1
            result = all_syncs[store].sync()
            if result == 0 and self.primary is not None:
                all_syncs[store].last_sync = all_syncs[self.primary.name].last_sync
            return result
        else:
            return_codes = []
            for to_sync in self.syncs:
                if to_sync.store.name != store:
                    return_codes.append(to_sync.sync())
            return 1 if len(return_codes) == 0 else min(abs(x) for x in return_codes)


class ToSync(Base):
    """
    Last time the dataset was synced to specific data store.

    This should only exist for datasets being synced.
    """

    __tablename__ = "to_sync"

    dataset_name = Column(
        String, ForeignKey("dataset.name"), nullable=False, primary_key=True
    )
    dataset = relationship("Dataset", back_populates="syncs")
    store_name = Column(
        String, ForeignKey("data_store.name"), nullable=False, primary_key=True
    )
    store = relationship("DataStore", back_populates="syncs")
    last_sync = Column(DateTime)

    def __repr__(
        self,
    ):
        """Represent to_sync as string."""
        return f"ToSync(dataset={self.dataset}, store={self.store}, last_sync={self.last_sync})"

    @property
    def path(
        self,
    ):
        """Path to dataset on data store."""
        if self.store.type == "disc":
            return f"/Volumes/{self.store_name}/data-archive/{self.dataset_name}/"
        if self.store.type == "ssh":
            return f"/Volumes/{self.store_name}/data-archive/{self.dataset_name}/"

    def sync(self, link=None):
        """Sync data in dataset from/to the store."""
        if self.dataset.archived:
            raise ValueError("Cannot sync an archived dataset.")
        if link is None:
            link = self.store.get_connection()
        if link is None:
            return 1

        from_local = self.store != self.dataset.primary
        if not from_local and self.store.is_archive:
            raise ValueError("Primary data store should not be an archive.")
        rc = link.sync(self.dataset.name, from_local=from_local)
        if rc == 0:
            self.last_sync = datetime.now()
        return rc


@lru_cache
def get_engine(filename="~/.config/dsync.sqlite"):
    """Get the SQLAlchemy Enginge interacting with the database (one per session)."""
    filename = op.abspath(op.expandvars(op.expanduser(filename)))
    database = "sqlite+pysqlite:///" + filename
    engine = create_engine(database, echo=False, future=True)

    Base.metadata.create_all(engine)
    return engine


def in_session(func):
    """Wrap functions that need to run in a database session."""

    @wraps(func)
    def wrapped(*args, database="~/.config/dsync.sqlite", session=None, **kwargs):
        if session is not None:
            return func(*args, session=session, **kwargs)
        engine = get_engine(database)
        with Session(engine) as session:
            res = func(*args, session=session, **kwargs)
            session.commit()
        return res

    return wrapped
