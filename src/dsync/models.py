"""Define dsync database models."""
import os.path as op
from functools import lru_cache, wraps

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.orm import Session, declarative_base, relationship

Base = declarative_base()


class Remote(Base):
    """Data storage location."""

    __tablename__ = "remote"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    ssh = Column(String, default="")
    syncs = relationship(
        "ToSync",
        back_populates="remote",
        cascade="all",
    )

    def __repr__(
        self,
    ):
        """Represent remote as string."""
        return f"Remote(id={self.id}, name={self.name})"


class Dataset(Base):
    """Dataset to be synced across remotes."""

    __tablename__ = "dataset"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    archived = Column(Boolean, default=False)
    primary_id = Column(Integer, ForeignKey("remote.id"), nullable=True, default=None)
    primary = relationship("Remote")
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
        return f"Dataset(id={self.id}, name={self.name})"


class ToSync(Base):
    """
    Last time the dataset was synced to specific remote.

    This should only exist for datasets being synced.
    """

    __tablename__ = "to_sync"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("dataset.id"), nullable=False)
    dataset = relationship("Dataset", back_populates="syncs")
    remote_id = Column(Integer, ForeignKey("remote.id"), nullable=False)
    remote = relationship("Remote", back_populates="syncs")
    last_sync = Column(DateTime)

    def __repr__(
        self,
    ):
        """Represent to_sync as string."""
        return f"ToSync(id={self.id}, dataset={self.dataset}, remote={self.remote})"


@lru_cache
def get_engine(filename="~/.config/dsync.sqlite"):
    """Get the SQLAlchemy Enginge interacting with the database (one per session)."""
    filename = op.abspath(op.expandvars(op.expanduser(filename)))
    database = "sqlite+pysqlite:///" + filename
    engine = create_engine(database, echo=True, future=True)

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
