"""Define dsync database models."""
import os.path as op
from datetime import datetime
from functools import lru_cache, wraps
from subprocess import run

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    String,
    create_engine,
)
from sqlalchemy.orm import Session, declarative_base, relationship

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
        for try_link in self.link.split(","):
            if self.type == "ssh":
                if (
                    run(["ssh", try_link, "-oBatchMode=yes", "-q", "echo"]).returncode
                    == 0
                ):
                    return try_link
            elif self.type == "disc":
                directory = f"/Volumes/{try_link}/data-archive/"
                if op.isdir(directory):
                    return directory
            else:
                raise ValueError("Unrecognised data store type")
        return None


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

    @property
    def local_path(
        self,
    ):
        """
        Path to local version of dataset.

        The data should exist in this location unless `self.archive` is True.
        """
        return op.join(op.expanduser("~/Work/data"), self.name) + "/"

    def __repr__(
        self,
    ):
        """Represent dataset as string."""
        return f"Dataset(name={self.name})"

    def sync(self, session, store_links):
        """Sync this dataset with the given store links."""
        if self.primary is not None:
            if self.primary not in store_links:
                link = self.primary.get_connection()
            else:
                link = store_links[self.primary]
            if link is None:
                raise ValueError(
                    f"Connection to primary store {self.primary_name} "
                    + f"is not available for {self.name}."
                )
            to_sync = session.query(ToSync).get((self.name, self.primary_name))
            to_sync.sync(link)

        return_codes = []
        for remote, link in store_links.items():
            if remote == self.primary or link is None:
                continue
            to_sync = session.query(ToSync).get((self.name, remote.name))
            if to_sync is None:
                if remote.is_archive:
                    to_sync = ToSync(dataset=self, store=remote)
                    session.add(to_sync)
                else:
                    continue
            return_codes.append(to_sync.sync(link))
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

    def sync(self, link_name):
        """Sync data in dataset from/to the store."""
        if self.dataset.archived:
            raise ValueError("Cannot sync an archived dataset.")
        if link_name is None:
            raise ValueError(
                f"Trying to sync with an unavailable data store {self.store_name}"
            )

        if self.store.type == "disc":
            store_path = f"{link_name}/{self.dataset.name}/"
        elif self.store.type == "ssh":
            store_path = f"{link_name}:Work/data/{self.dataset.name}/"

        if self.store == self.dataset.primary:
            if self.store.is_archive:
                raise ValueError("Primary data store should not be an archive.")
            cmd = ["rsync", "-aP", store_path, self.dataset.local_path]
        else:
            cmd = ["rsync", "-aP", self.dataset.local_path, store_path]
        print("running ", " ".join(cmd))
        rc = run(cmd).returncode
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
