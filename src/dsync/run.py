"""CLI interface for dsync."""
import os.path as op

import click

from .models import Dataset, Remote, ToSync, in_session


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
        primary=primary,
    )
    if primary is None and not op.isdir(new_dataset.local_path):
        raise ValueError("Cannot start syncing a dataset that does not exist locally.")
    session.add(new_dataset)


@cli.command
@click.argument("name")
@in_session
def add_remote(name, session):
    """Add locally existing dataset to database."""
    new_remote = Remote(
        name=name,
        ssh=name,
    )
    session.add(new_remote)


@cli.command
@click.argument("dataset")
@click.argument("remote")
@in_session
def sync_to(dataset, remote, session):
    """Add locally existing dataset to database."""
    remote_obj = session.query(Remote).get(remote)
    if remote_obj is None:
        raise ValueError(
            f"Unrecognised remote: {remote}. Create new remote using add-remote."
        )
    dataset_obj = session.query(Dataset).get(dataset)
    if dataset_obj is None:
        raise ValueError(
            f"Unrecognised dataset: {dataset}. Create new dataset using add-dataset."
        )
    sync_obj = session.query(ToSync).get((dataset, remote))
    if sync_obj is not None:
        click.echo(f"{dataset} is already syncing to {remote}")
    else:
        session.add(ToSync(dataset=dataset_obj, remote=remote_obj))


@cli.command
@in_session
def archive(session):
    """Copy all datasets to archive."""
    for dataset in session.query(Dataset).all():
        if not dataset.archived:
            print(f"TODO, archive: {dataset}")
