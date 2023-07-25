"""CLI interface for dsync."""
import os.path as op

import click
import rich
from rich.table import Table

from .models import Dataset, DataStore, ToSync, in_session
from .query import datasets, last_sync, stores


@click.group
def cli():
    """Top-level CLI for dsync."""
    pass


@cli.command
@click.argument("name")
@click.argument("description")
@click.option("-p", "--primary", default=None)
@in_session
def add_dataset(name, description, session, primary=None):
    """Add locally existing dataset to database."""
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
@click.argument("dataset")
@click.argument("remote")
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
    dataset_obj = session.query(Dataset).get(dataset)
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
@click.argument("dataset")
@click.argument("primary", default=None, required=False)
@click.option("--skip-sync", is_flag=True, default=False)
@in_session
def set_primary(dataset, primary, session, skip_sync=False):
    """
    Set the primary of dataset to a remote data store.

    If the primary is not provided, it will be set to the local directory system.
    """
    dataset = datasets(session, name=dataset)
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
    """List all data stores and datasets."""
    list_stores(session)
    list_datasets(session)


def list_stores(session):
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


def list_datasets(session):
    """List all datasets."""
    all_stores = stores(session)
    all_datasets = datasets(session)

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
@click.option("-d", "--dataset")
@click.option("-s", "--store")
@in_session
def sync(session, dataset=None, store=None):
    """Sync any dataset to any remote."""
    all_datasets = datasets(session, name=dataset, as_list=True)
    all_stores = stores(session, name=store, as_list=True)

    store_links = {s: s.get_connection() for s in all_stores}
    missing = ", ".join(
        [key.name for key, value in store_links.items() if value is None]
    )
    if len(missing) > 0:
        rich.print(f"Skipping missing data stores: {missing}")

    for ds_iter in all_datasets:
        try:
            rc = ds_iter.sync(session, store_links)
            if rc != 0:
                raise ValueError(f"Failed to sync {dataset}")
        except ValueError as e:
            if dataset is not None:
                raise e


@cli.command
@click.argument("dataset")
@in_session
def archive(dataset, session):
    """
    Mark dataset as mainly existing on the archive.

    It will no longer be synced and can be safely deleted from other machines.
    """
    dataset_obj = datasets(session, name=dataset)
    if dataset_obj.archived:
        raise ValueError(f"Dataset '{dataset}' is already archived.")
    if dataset_obj.primary is not None:
        sync.callback(session=session, dataset=dataset)
    dataset_obj.update_latest_edit()
    for sync_obj in dataset_obj.syncs:
        if sync_obj.store.is_archive and (
            sync_obj.last_sync is None or (sync_obj.last_sync < dataset_obj.latest_edit)
        ):
            raise ValueError(
                f"Can not archive dataset '{dataset}', "
                f"because sync to store '{sync_obj.store.name}' is not up to date."
            )
    rich.print("archiving", dataset_obj)
    dataset_obj.archived = True


@cli.command
@click.argument("dataset")
@in_session
def unarchive(dataset, session):
    """
    Retrieve dataset from the archive.

    The primary will always be reset to local.
    """
    dataset_obj = datasets(session, name=dataset)
    if not dataset_obj.archived:
        raise ValueError(f"Dataset '{dataset}' is not archived already.")

    for store in stores(session):
        if not store.is_archive:
            continue
        link = store.get_connection()
        if link is None:
            continue
        if link.sync(dataset, from_local=False) == 0:
            break
    else:
        raise ValueError(f"Could not retrieve '{dataset}' from archive.")

    rich.print("unarchiving", dataset_obj)
    dataset_obj.archived = False
    dataset_obj.primary = None
