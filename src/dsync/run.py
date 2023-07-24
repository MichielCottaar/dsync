"""CLI interface for dsync."""
import datetime
import os.path as op

import click

from .models import Dataset, DataStore, ToSync, in_session


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
    """Add remote to database."""
    new_remote = DataStore(
        name=name,
        ssh=name,
        type="ssh",
    )
    session.add(new_remote)


@cli.command
@click.argument("dataset")
@click.argument("remote")
@in_session
def add_sync(dataset, remote, session):
    """Intruct dsync to sync dataset with remote from now on."""
    remote_obj = session.query(DataStore).get(remote)
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
    sync(session, dataset=dataset, remote=remote)


@cli.command
@click.option("-d", "--dataset")
@click.option("-r", "--remote")
@in_session
def sync(session, dataset=None, remote=None):
    """Sync any dataset to any remote."""
    if dataset is not None:
        datasets = [session.query(Dataset).get(dataset)]
        if datasets[0] is None:
            raise ValueError(f"Trying to sync unknown dataset {dataset}.")
    else:
        datasets = session.query(Dataset).all()

    if remote is not None:
        remotes = [session.query(DataStore).get(remote)]
        if remotes[0] is None:
            raise ValueError(f"Trying to sync unknown remote {remote}.")
    else:
        remotes = session.query(DataStore).all()

    # test ssh connections to remote

    for ds_iter in datasets:
        for r_iter in remotes:
            to_sync = session.query(ToSync).get((ds_iter.name, r_iter.name))
            if to_sync is None:
                if dataset is not None and remote is not None:
                    raise ValueError(
                        f"Dataset {ds_iter} is not being synced to {r_iter}. "
                        + "Use `add-sync` to enable this."
                    )
                continue
            print(f"Syncing: {to_sync}")
            to_sync.last_sync = datetime.datetime.now()


@cli.command
@in_session
def archive(session):
    """Copy all datasets to archive."""
    for dataset in session.query(Dataset).all():
        if not dataset.archived:
            print(f"TODO, archive: {dataset}")
