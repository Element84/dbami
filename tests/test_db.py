import random
import string
from pathlib import Path
import pytest

from dbami.db import DB, Migration
from dbami.util import syncrun


@pytest.fixture
def empty_project(tmp_path: Path):
    db: DB = DB.new_project(tmp_path)
    return db


@pytest.fixture
def project(tmp_path: Path):
    # create some migrations before instantiating the DB instance
    # to ensure we test the migration load process
    migrations_dir = DB.project_migrations(tmp_path)
    migrations_dir.mkdir()
    migrations_dir.joinpath("00_migration.up.sql").touch()
    migrations_dir.joinpath("00_migration.down.sql").touch()
    migrations_dir.joinpath("01_migration.up.sql").touch()
    db: DB = DB.new_project(tmp_path)
    db.schema.path.write_text(
        """
CREATE TABLE IF NOT EXISTS schema_version (
  version integer,
  applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX ON schema_version (version);
CREATE INDEX ON schema_version (applied_at);

INSERT INTO schema_version (version) VALUES (4);
"""
    )
    db.new_migration("migration")
    db.new_migration("migration")
    db.new_migration("migration")
    db.new_fixture("a_fixture")
    db.new_test("a_test")
    db.new_test("b_test")
    return db


@pytest.fixture
def tmp_db():
    db_postfix = "".join(random.choices(string.ascii_letters, k=5))
    db_name = f"dbami_test_{db_postfix.lower()}"

    try:
        syncrun(DB.create_database(db_name))
        yield db_name
    finally:
        syncrun(DB.drop_database(db_name))
        pass


def test_no_project(tmp_path: Path):
    with pytest.raises(FileNotFoundError) as exc_info:
        DB(tmp_path)
    assert str(exc_info.value).startswith("Schema does not exist or is wrong type:")


def test_init(tmp_path: Path):
    db: DB = DB.new_project(tmp_path)
    assert db.fixtures_dir.is_dir()
    assert db.migrations_dir.is_dir()
    assert db.schema_file.is_file()
    assert db.tests_dir.is_dir()


def test_new_migration(empty_project):
    migration_name = "a-migration"
    empty_project.new_migration(migration_name)
    new_migration = max(empty_project.migrations.values())
    assert new_migration.name == migration_name
    assert new_migration.up.path.is_file()
    assert new_migration.down.path.is_file()
    assert new_migration.id == 0
    assert 0 in empty_project.migrations


def test_new_fixture(empty_project):
    fixture_name = "a_fixture"
    empty_project.new_fixture(fixture_name)
    assert fixture_name in empty_project.fixtures
    fixture = empty_project.fixtures[fixture_name]
    assert fixture.name == fixture_name
    assert fixture.path.is_file()


def test_duplicate_fixture(empty_project):
    fixture_name = "a_fixture"
    empty_project.new_fixture(fixture_name)
    with pytest.raises(FileExistsError) as exc_info:
        empty_project.new_fixture(fixture_name)
    assert str(exc_info.value).startswith("Cannot create fixture, already exists:")


def test_new_test(empty_project):
    test_name = "a_test"
    empty_project.new_test(test_name)
    assert test_name in empty_project.tests
    test = empty_project.tests[test_name]
    assert test.name == test_name
    assert test.path.is_file()


def test_duplicate_test(empty_project):
    test_name = "a_test"
    empty_project.new_test(test_name)
    with pytest.raises(FileExistsError) as exc_info:
        empty_project.new_test(test_name)
    assert str(exc_info.value).startswith("Cannot create test, already exists:")


def test_project(project):
    assert len(project.migrations.values()) == 5
    assert {0, 1, 2, 3, 4} == set(project.migrations.keys())
    assert project.migrations[0].down is not None


def test_create_database(tmp_db):
    assert tmp_db


@pytest.mark.asyncio
async def test_get_version_not_migrated(tmp_db):
    assert await DB.get_current_version(database=tmp_db) is None


@pytest.mark.asyncio
async def test_load_schema(tmp_db, project):
    await project.load_schema(database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 4


@pytest.mark.asyncio
async def test_migrate(tmp_db, project):
    await project.migrate(database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 4


@pytest.mark.asyncio
async def test_rollback(tmp_db, project):
    await project.load_schema(database=tmp_db)
    await project.migrate(target=2, database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 2


@pytest.mark.asyncio
async def test_rollback_no_down(tmp_db, project):
    await project.load_schema(database=tmp_db)
    with pytest.raises(ValueError) as exc_info:
        await project.migrate(target=1, database=tmp_db)
    assert str(exc_info.value) == (
        "Cannot rollback from version 4 to 1: "
        "one or more migrations do not have down files",
    )


@pytest.mark.asyncio
async def test_migrate_no_migrations(tmp_db, project):
    await project.load_schema(database=tmp_db)
    await project.migrate(database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 4


@pytest.mark.asyncio
async def test_migrate_different_schema_for_versions(tmp_db, project):
    table = "schema.table"
    await project.migrate(schema_version_table=table, database=tmp_db)
    assert (
        await project.get_current_version(
            schema_version_table=table,
            database=tmp_db,
        )
        == 4
    )


@pytest.mark.asyncio
async def test_migrate_newer_than_migrations(tmp_db, project):
    await project.load_schema(database=tmp_db)
    del project.migrations[4]
    await project.migrate(database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 4


@pytest.mark.asyncio
async def test_migrate_unknown_target(tmp_db, project):
    target = 10
    with pytest.raises(ValueError) as exc_info:
        await project.migrate(target, database=tmp_db)
    assert (
        str(exc_info.value) == f"Target migration ID '{target}' has no known migration"
    )


@pytest.mark.asyncio
async def test_migrate_rollback_from_unknown_schema(tmp_db, project):
    target = 3
    await project.migrate(target, database=tmp_db)
    del project.migrations[target]
    with pytest.raises(ValueError) as exc_info:
        await project.migrate(0, database=tmp_db)
    assert (
        str(exc_info.value)
        == f"Schema version '{target}' does not have associated migration"
    )


@pytest.mark.asyncio
async def test_yield_unapplied_migrations(tmp_db, project) -> None:
    unapplied: list[Migration] = [
        m async for m in project.yield_unapplied_migrations(database=tmp_db)
    ]
    assert len(unapplied) == 5


@pytest.mark.asyncio
async def test_dump(tmp_db, project) -> None:
    empty_dump: str = """--
-- PostgreSQL database dump
--

-- Dumped from database version 15.3 (Debian 15.3-1.pgdg110+1)
-- Dumped by pg_dump version 15.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- PostgreSQL database dump complete
--

"""
    rc, dump = await project.dump("-d", tmp_db)
    assert rc == 0
    assert dump == empty_dump


@pytest.mark.asyncio
async def test_load_fixture(tmp_db, project) -> None:
    await project.load_fixture("a_fixture", database=tmp_db)
    assert True


@pytest.mark.asyncio
async def test_load_fixture_unknown(tmp_db, project) -> None:
    with pytest.raises(FileNotFoundError) as exc_info:
        await project.load_fixture("bad_fixture", database=tmp_db)
    assert str(exc_info.value).startswith("Unknown fixture:")
