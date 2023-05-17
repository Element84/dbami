from pathlib import Path

from dbami.db import SqlFile


def test_init():
    SqlFile(Path("/some/test/path.sql"))
    assert True


def test_lt():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/02_path.sql"))
    assert one < two


def test_not_lt():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/02_path.sql"))
    assert not two < one


def test_gt():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/02_path.sql"))
    assert two > one


def test_not_gt():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/02_path.sql"))
    assert not one > two


def test_le():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/02_path.sql"))
    assert one <= two


def test_le2():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/01_path.sql"))
    assert one <= two


def test_not_le():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/02_path.sql"))
    assert not two <= one


def test_ge():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/02_path.sql"))
    assert two >= one


def test_ge2():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/01_path.sql"))
    assert one >= two


def test_not_ge():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/02_path.sql"))
    assert not one >= two


def test_eq():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/01_path.sql"))
    assert one == two


def test_not_eq():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/02_path.sql"))
    assert not one == two


def test_ne():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/02_path.sql"))
    assert one != two


def test_not_ne():
    one = SqlFile(Path("/some/test/01_path.sql"))
    two = SqlFile(Path("/some/test/01_path.sql"))
    assert not one != two
