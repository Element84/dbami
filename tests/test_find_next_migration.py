from pathlib import Path

import pytest

from dbami.db import Migration, find_next_migration


@pytest.fixture
def migrations():
    migrations = {}
    parent = None

    for index in range(10):
        migrations[index] = parent = Migration.from_up_path(
            Path(f"/some/path/{index}_name.up.sql"),
            parent=parent,
        )

    return migrations


def test_from_none(migrations):
    m = find_next_migration(-1, migrations)
    assert m is not None
    assert m.id == 0


def test_from_3(migrations):
    m = find_next_migration(3, migrations)
    print(migrations.keys())
    assert m is not None
    assert m.id == 4


def test_from_12(migrations):
    m = find_next_migration(12, migrations)
    assert m is None


def test_from_neg3(migrations):
    with pytest.raises(ValueError) as exc_info:
        find_next_migration(-3, migrations)
    assert str(exc_info.value).startswith("No migration path from schema version")
