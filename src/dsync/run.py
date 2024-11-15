"""CLI interface for dsync."""
import os.path as op
from functools import partial

import click
import rich
from rich.table import Table

from .models import Dataset, DataStore, ToSync, in_session
from .query import (
    complete_datasets,
    complete_stores,
    datasets,
    get_dataset,
    last_sync,
    stores,
)


@click.group
def cli():
    """Top-level CLI for dsync."""
    pass


@cli.command
@click.argument("name")
@click.argument("description", required=False, default=None)
@click.option(
    "-p",
    "--primary",
    default=None,
    shell_complete=partial(complete_stores, only_remotes=True),
)
@in_session
def add_dataset(name, description, session, primary=None):
    """Add locally existing dataset to database."""
    if description is None:
        description = name
    if isinstance(primary, str):
        primary = stores(session, name=primary)
    new_dataset = Dataset(
        name=name,
        description=description,
        primary=primary,
    )
    if primary is None and not op.isdir(new_dataset.local_path):
        raise ValueError("Cannot start syncing a dataset that does not exist locally.")
    session.add(new_dataset)
    if primary is not None:
        add_sync.callback(new_dataset.name, primary.name, session=session)


@cli.command
@click.argument("name")
@click.option("-t", "--type", default="ssh", show_default=True)
@click.option("-l", "--link")
def add_remote(name, type="ssh", link=None):
    """Add remote to database."""
    add_data_store(name, type, False, link)


@cli.command
@click.argument("name")
@click.option("-t", "--type", default="disc", show_default=True)
@click.option("-l", "--link")
def add_archive(name, type="disc", link=None):
    """Add archive to database."""
    add_data_store(name, type, True, link)


@in_session
def add_data_store(name, type, is_archive, link, session):
    """Add data store (remote or archive) to database."""
    if link is None:
        link = name
    if type not in ("disc", "ssh"):
        raise ValueError(f"Data store type should be one of disc/ssh, not {type}")
    new_remote = DataStore(name=name, link=link, type=type, is_archive=is_archive)
    session.add(new_remote)


@cli.command
@click.argument("remote", shell_complete=partial(complete_stores, only_remotes=True))
@click.option("-d", "--dataset", shell_complete=complete_datasets, default=None)
@in_session
def add_sync(dataset, remote, session):
    """Instruct dsync to sync dataset with remote from now on."""
    remote_obj = session.query(DataStore).get(remote)
    if remote_obj is None:
        raise ValueError(
            f"Unrecognised remote: {remote}. Create new remote using add-remote."
        )
    if remote_obj.is_archive:
        raise ValueError(
            "All datasets will be archived. "
            + f"No need to manually add archive datastore {remote_obj.name}."
        )
    dataset_obj = get_dataset(session, dataset)
    if dataset_obj is None:
        raise ValueError(
            f"Unrecognised dataset: {dataset}. Create new dataset using add-dataset."
        )
    sync_obj = session.query(ToSync).get((dataset, remote))
    if sync_obj is not None:
        click.echo(f"{dataset} is already syncing to {remote}")
    else:
        session.add(ToSync(dataset=dataset_obj, store=remote_obj))
    sync.callback(session=session, dataset=dataset, store=remote)


@cli.command
@click.argument(
    "primary",
    default=None,
    required=False,
    shell_complete=partial(complete_stores, only_remotes=True),
)
@click.option("-d", "--dataset", shell_complete=complete_datasets, default=None)
@click.option("--skip-sync", is_flag=True, default=False)
@in_session
def set_primary(dataset, primary, session, skip_sync=False):
    """
    Set the primary of dataset to a remote data store.

    If the primary is not provided, it will be set to the local directory system.
    """
    dataset = get_dataset(session, name=dataset)
    if dataset.archived:
        raise ValueError(
            "Cannot set primary of archived dataset. Please run `dsync unarchive` on it first."
        )
    if dataset is None:
        raise ValueError(f"Cannot set primary of non-existant dataset {dataset}.")
    if primary is not None:
        primary = stores(session, name=primary)
        if primary is None:
            raise ValueError(f"Cannot set primary to an unknown remote {primary}.")
        if primary.is_archive:
            raise ValueError(
                f"Primary cannot be set to an archive data store, such as {primary}."
            )
    if dataset.primary == primary:
        rich.print("New primary is the same as the old one. Doing nothing...")
        return

    # Sync from old primary to new primary
    if not skip_sync:
        sync.callback(
            session=session,
            dataset=dataset.name,
            store=dataset.primary.name if primary is None else primary.name,
        )

    dataset.primary = primary


@cli.command
@in_session
def list(session):
    """List the datasets to stdout."""
    for dataset in datasets(session):
        if not dataset.archived:
            print(dataset.name)


@cli.command
@click.option(
    "-d", "--dataset", shell_complete=partial(complete_datasets, archived=False)
)
@in_session
def get_remotes(dataset, session):
    """Print the primary of given dataset to stdout."""
    obj = get_dataset(session, name=dataset, verbose=False)
    if obj is None:
        if dataset is None:
            raise ValueError(
                "Not currently in a dataset. Please set `-d/--dataset` explicitly."
            )
        else:
            raise ValueError(f"Dataset '{dataset}' does not exist.")
    if obj.archived:
        raise ValueError(f"Archived datasets like '{obj}' do not have remotes.")
    if obj.primary is None:
        primary = "local"
    else:
        primary = obj.primary.name
    print(primary)
    if primary != "local":
        print("local")

    for to_sync in obj.syncs:
        if not to_sync.store.is_archive and to_sync.store.name != primary:
            print(to_sync.store.name)


@cli.command
@in_session
def summary(session, test=False):
    """Summarises all data stores and datasets."""
    if test:
        summary_stores(session)
    summary_datasets(session)


def summary_stores(session):
    """List all data stores (remotes & archives)."""
    remotes = Table(title="Remote data stores")
    for header in ("name", "link", "works"):
        remotes.add_column(header)
    archives = Table(title="Archives")
    for header in ("name", "directory", "works"):
        archives.add_column(header)

    for store in stores(session=session):
        link = store.get_connection()
        works = "❌" if link is None else "✅︎"
        if store.is_archive:
            archives.add_row(store.name, store.link, works)
        else:
            remotes.add_row(store.name, store.link, works)
    rich.print(archives)
    rich.print(remotes)


def summary_datasets(session):
    """List all datasets."""
    all_stores = stores(session)
    all_datasets = sorted(
        sorted(datasets(session), key=lambda d: d.name), key=lambda d: d.archived
    )

    table = Table(title="Datasets")
    for header in ("name", "primary", "latest edit", "local"):
        table.add_column(header)
    for store in all_stores:
        table.add_column(store.name)

    for dataset in all_datasets:
        if dataset.archived:
            row = [
                dataset.name,
                "📁",
                dataset.latest_edit.strftime("%Y-%m-%d %H:%M"),
            ] + [""] * (len(all_stores) + 1)
            table.add_row(*row)
            continue

        row = [dataset.name]
        if dataset.primary is None:
            # local version is the primary one
            row.extend(["local", "primary"])
        else:
            ls = last_sync(dataset, dataset.primary, session)
            row.extend(
                [
                    dataset.primary.name,
                    ls if isinstance(ls, str) else ls.strftime("%Y-%m-%d %H:%M"),
                ]
            )
        dataset.update_latest_edit()
        row.insert(2, dataset.latest_edit.strftime("%Y-%m-%d %H:%M"))

        for store in all_stores:
            if store == dataset.primary:
                row.append("primary")
            else:
                ls = last_sync(dataset, store, session)
                row.append(
                    ""
                    if ls is None
                    else (ls if isinstance(ls, str) else ls.strftime("%Y-%m-%d %H:%M"))
                )
        table.add_row(*row)

    rich.print(table)


@cli.command
@click.option("-d", "--dataset", shell_complete=complete_datasets)
@click.option("-s", "--store", shell_complete=complete_stores)
@in_session
def sync(session, dataset=None, store=None):
    """Sync any dataset to any remote."""
    all_datasets = [get_dataset(session, dataset)]
    if all_datasets[0] is None:
        if dataset is not None:
            raise ValueError(f"Trying to sync unknown dataset '{dataset}'")
        all_datasets = datasets(session)

    for ds_iter in all_datasets:
        try:
            rc = ds_iter.sync(session, store)
            if rc != 0:
                raise ValueError(f"Failed to sync {ds_iter}")
        except ValueError:
            if len(all_datasets) == 1:
                raise


@cli.command
@click.option("-d", "--dataset", shell_complete=complete_datasets)
@in_session
def archive(dataset, session):
    """
    Mark dataset as mainly existing on the archive.

    It will no longer be synced and can be safely deleted from other machines.
    """
    dataset_obj = get_dataset(session, name=dataset)
    if dataset_obj.archived:
        raise ValueError(f"Dataset '{dataset_obj.name}' is already archived.")
    if dataset_obj.primary is not None:
        sync.callback(session=session, dataset=dataset, store=dataset_obj.primary.name)
    dataset_obj.update_latest_edit()
    for sync_obj in dataset_obj.syncs:
        if sync_obj.store.is_archive and (
            sync_obj.last_sync is None or (sync_obj.last_sync < dataset_obj.latest_edit)
        ):
            raise ValueError(
                f"Can not archive dataset '{dataset_obj.name}', "
                f"because sync to store '{sync_obj.store.name}' is not up to date. "
                "Please run `dsync sync` with that remote/archive available."
            )
    rich.print("archiving", dataset_obj)
    dataset_obj.archived = True


@cli.command
@click.option(
    "-d", "--dataset", shell_complete=partial(complete_datasets, archived=True)
)
@in_session
def unarchive(dataset, session):
    """
    Retrieve dataset from the archive.

    The primary will always be reset to local.
    """
    dataset_obj = get_dataset(session, name=dataset)
    if not dataset_obj.archived:
        raise ValueError(f"Dataset '{dataset_obj.name}' is not archived already.")

    for store in stores(session):
        if not store.is_archive:
            continue
        link = store.get_connection()
        if link is None:
            continue
        if link.sync(dataset_obj.name, from_local=False) == 0:
            break
    else:
        raise ValueError(f"Could not retrieve '{dataset_obj.name}' from archive.")

    rich.print("unarchiving", dataset_obj)
    dataset_obj.archived = False
    dataset_obj.primary = None


@cli.command
@click.argument("paths", nargs=-1, type=click.Path(exists=True))
@click.option("-s", "--store", shell_complete=complete_stores)
def put(paths, store):
    """
    Copy a specific file or directory from the local machine to the given store.

    This allows files to be transfered from another machine to the primary store.
    By default, the target store will be the primary
    (raises an error if the local machine is the primary).
    """
    transfer_specific_files(paths, store, from_local=True)


@cli.command
@click.argument("paths", nargs=-1, type=click.Path(writable=True))
@click.option("-s", "--store", shell_complete=complete_stores)
def get(paths, store):
    """
    Copy a specific file or directory from a given store to the local machine.

    This allows files to be transfered from another machine to the primary store.
    By default, the target store will be the primary
    (raises an error if the local machine is the primary).
    """
    transfer_specific_files(paths, store, from_local=False)


@in_session
def transfer_specific_files(paths, store, from_local, session):  # noqa: C901
    """
    Transfer specific files from/to a remote store.

    Used by `dsync put` and `dsync get`.
    """
    dataset = get_dataset(session, current_directory=paths[0])
    if dataset is None:
        raise ValueError(
            f"Can only transfer files that are part of a dataset, which '{paths[0]}' is not."
        )
    if dataset.archived:
        raise ValueError(
            f"Dataset '{dataset.name}' is archived. "
            "Please run `dsync unarchive` before transfering data."
        )
    for path in paths[1:]:
        if dataset != get_dataset(session, current_directory=path, verbose=False):
            raise ValueError("Not all requested paths are in the same dataset.")

    if store is None:
        store = dataset.primary
        if store is None:
            raise ValueError(
                f"Local storage is the primary for {dataset.name}. "
                "Please set -s/--store to select a target."
            )
    else:
        store = stores(session, name=store)

    if not store.is_archive:
        sync_obj = session.query(ToSync).get((dataset.name, store.name))
        if sync_obj is None:
            rich.print(f"Sending data to unsynced remote {store.name}")

    connection = store.get_connection()
    if connection is None:
        raise ValueError(f"Unable to set up connection to {store.name}.")

    for path in paths:
        relpath = op.relpath(
            op.abspath(path), op.expanduser(f"~/Work/data/{dataset.name}")
        )
        if (op.exists(path) and op.isdir(path)) or (  # I know it is a path
            not op.exists(path) and path.endswith("/")  # User told me its a path
        ):
            relpath = relpath + "/"
        connection.sync(dataset.name, relpath, from_local=from_local)
