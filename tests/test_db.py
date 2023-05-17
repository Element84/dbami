import io
from pathlib import Path

import pytest

from dbami.db import DB, Migration


def test_no_project(tmp_path: Path):
    with pytest.raises(FileNotFoundError) as exc_info:
        DB(tmp_path)
    assert str(exc_info.value).startswith("Schema does not exist or is wrong type:")


def test_init(tmp_path: Path):
    db: DB = DB.new_project(tmp_path)
    assert db.fixtures_dir.is_dir()
    assert db.migrations_dir.is_dir()
    assert db.schema_file.is_file()


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
        "one or more migrations do not have down files"
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
async def test_load_fixture(tmp_db, project) -> None:
    await project.load_fixture("a_fixture", database=tmp_db)
    assert True


@pytest.mark.asyncio
async def test_load_fixture_unknown(tmp_db, project) -> None:
    with pytest.raises(FileNotFoundError) as exc_info:
        await project.load_fixture("bad_fixture", database=tmp_db)
    assert str(exc_info.value).startswith("Unknown fixture:")


@pytest.mark.asyncio
async def test_verify_matches(project) -> None:
    same = await project.verify()
    assert same


@pytest.mark.asyncio
async def test_verify_different_schema(project) -> None:
    project.migrations[4].up.path.write_text(
        """
CREATE SCHEMA some_schema;
CREATE TABLE some_schema.some_table (
    a_col integer PRIMARY KEY,
    b_col text UNIQUE
);
"""
    )
    output = io.StringIO()
    same = await project.verify(output=output)
    assert not same
    output.seek(0)
    outstr = output.read()
    assert outstr.startswith("--- schema.sql")


@pytest.mark.asyncio
async def test_verify_different_version(project) -> None:
    project.new_migration("migration")
    output = io.StringIO()
    same = await project.verify(output=output)
    assert not same
    output.seek(0)
    outstr = output.read()
    print(outstr)
    assert outstr == "Version from schema doesn't match that from migrations: 4 != 5\n"
