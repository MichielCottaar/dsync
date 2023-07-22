"""Allow for dry runs for testing."""
from contextlib import contextmanager

DRYRUN = False


@contextmanager
def dry_run():
    """Any commands within this context manager will not be run."""
    global DRYRUN
    prev = DRYRUN
    DRYRUN = False
    yield DRYRUN
    DRYRUN = prev
