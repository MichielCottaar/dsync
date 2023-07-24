"""Define dsync database models."""
import os.path as op
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

Base = declarative_base()


class DataStore(Base):
    """Data storage location (ssh/disc)."""

    __tablename__ = "data_store"

    name = Column(String, primary_key=True)
    link = Column(String, default="")
    type = Column(String, default="ssh")  # ssh or disc
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


class Dataset(Base):
    """Dataset to be synced across data stores."""

    __tablename__ = "dataset"

    name = Column(String, primary_key=True)
    description = Column(String)
    archived = Column(Boolean, default=False)
    last_home_archive = Column(DateTime, default=None)
    last_work_archive = Column(DateTime, default=None)
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
        Return path to local version of dataset.

        The data should exist in this location unless `self.archive` is True.
        """
        return op.join(op.expanduser("~/Work/data"), self.name)

    def __repr__(
        self,
    ):
        """Represent dataset as string."""
        return f"Dataset(name={self.name})"


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
            func(*args, session=session, **kwargs)
        engine = get_engine(database)
        with Session(engine) as session:
            func(*args, session=session, **kwargs)
            session.commit()

    return wrapped
