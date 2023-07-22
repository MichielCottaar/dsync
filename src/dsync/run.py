"""CLI interface for dsync."""
import os.path as op

import click

from .models import Dataset, Remote, in_session


@click.group
def cli():
    """Top-level CLI for dsync."""
    pass


@cli.command
@click.argument("name")
@click.argument("description")
@in_session
def add_dataset(name, description, session, primary=None):
    """Add locally existing dataset to database."""
    new_dataset = Dataset(
        name=name,
        description=description,
    )
    if primary is None and not op.isdir(new_dataset.local_path):
        raise ValueError("Cannot start syncing a dataset that does not exist locally.")
    session.add(new_dataset)


@cli.command
@click.argument("name", prompt="Should match SSH name.")
@in_session
def add_remote(name, session):
    """Add locally existing dataset to database."""
    new_remote = Remote(
        name=name,
        ssh=name,
    )
    session.add(new_remote)
