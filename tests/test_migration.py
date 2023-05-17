from pathlib import Path

import pytest

from dbami.db import Migration, SqlFile


def test_init():
    _id = 1
    name = "path"
    up = SqlFile(Path("/some/test/01_path.up.sql"))
    down = SqlFile(Path("/some/test/01_path.down.sql"))
    str(Migration(_id, name, up, down))
    assert True


def test_from_up_path():
    _id = 1
    name = "path"
    up_path = Path("/some/test/01_path.up.sql")
    m = Migration.from_up_path(up_path)
    assert m.id == _id
    assert m.name == name
    assert m.up.path == up_path
    # we don't have down because it only gets set if it exists
    assert m.down is None
    assert m.parent is None
    assert m.child is None


def test_from_up_path_bad_path():
    up_path = Path("/some/test/path.sql")
    with pytest.raises(ValueError) as exc_info:
        Migration.from_up_path(up_path)
    assert str(exc_info.value).startswith(
        "Cannot extract migration ID and/or name from path",
    )


def test_with_parent():
    parent_path = Path("/some/test/01_path.up.sql")
    child_path = Path("/some/test/02_path.up.sql")
    parent = Migration.from_up_path(parent_path)
    child = Migration.from_up_path(child_path, parent=parent)
    assert child.parent is parent
    assert parent.child is child


def test_with_child():
    parent_path = Path("/some/test/01_path.up.sql")
    child_path = Path("/some/test/02_path.up.sql")
    child = Migration.from_up_path(child_path)
    parent = Migration.from_up_path(parent_path, child=child)
    assert child.parent is parent
    assert parent.child is child


def test_lt() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/02_path.up.sql"))
    assert one < two


def test_not_lt() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/02_path.up.sql"))
    assert not two < one


def test_gt() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/02_path.up.sql"))
    assert two > one


def test_not_gt() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/02_path.up.sql"))
    assert not one > two


def test_le() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/02_path.up.sql"))
    assert one <= two


def test_le2() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    assert one <= two


def test_not_le() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/02_path.up.sql"))
    assert not two <= one


def test_ge() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/02_path.up.sql"))
    assert two >= one


def test_ge2() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    assert one >= two


def test_not_ge() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/02_path.up.sql"))
    assert not one >= two


def test_eq() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    assert one == two


def test_not_eq() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/02_path.up.sql"))
    assert not one == two


def test_ne() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/02_path.up.sql"))
    assert one != two


def test_not_ne() -> None:
    one: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    two: Migration = Migration.from_up_path(Path("/some/test/01_path.up.sql"))
    assert not one != two
