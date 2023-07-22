"""Define dsync database models."""
from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Remote(Base):
    """Data storage location."""

    __tablename__ = "remote"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    is_local = Column(Boolean, default=False)
    is_archive = Column(Boolean, default=False)
    ssh = Column(String)
    disc_name = Column(String)
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
    primary = relationship("Remote")
    syncs = relationship(
        "ToSync",
        back_populates="dataset",
        cascade="all",
    )

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
    dataset = relationship("Dataset", back_populates="syncs")
    remote = relationship("Remote", back_populates="syncs")
    last_sync = Column(DateTime)

    def __repr__(
        self,
    ):
        """Represent to_sync as string."""
        return f"ToSync(id={self.id}, dataset={self.dataset}, remote={self.remote})"
