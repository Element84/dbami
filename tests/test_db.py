import io
from pathlib import Path
from typing import Union

import asyncpg
import pytest

from dbami import exceptions
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


def test_project_no_migrations(tmp_path: Path):
    db: DB = DB.new_project(tmp_path)
    db.migrations[0].up.path.unlink()
    with pytest.raises(FileNotFoundError) as exc_info:
        DB(tmp_path)
    assert (
        str(exc_info.value)
        == "Project is missing base migration file. Try reinitializing."
    )


def test_new_migration(empty_project):
    migration_name = "a-migration"
    empty_project.new_migration(migration_name)
    new_migration = max(empty_project.migrations.values())
    assert new_migration.name == migration_name
    assert new_migration.up.path.is_file()
    assert new_migration.down.path.is_file()
    assert new_migration.id == 1
    assert 1 in empty_project.migrations


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
async def test_get_version_not_migrated(tmp_db, project):
    assert await project.get_current_version(database=tmp_db) is None


@pytest.mark.asyncio
async def test_load_schema(tmp_db, project):
    await project.load_schema(database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 4


@pytest.mark.asyncio
async def test_load_schema_bad_sql(tmp_db, project):
    schema_file = project.schema.path
    schema = schema_file.read_text()
    schema += "\n\n CREATE TABLE bad_table (fk integer REFERENCES nothing);"
    schema_file.write_text(schema)
    with pytest.raises(asyncpg.UndefinedTableError):
        await project.load_schema(database=tmp_db)
    assert await project.get_current_version(database=tmp_db) is None


@pytest.mark.asyncio
async def test_load_schema_existing_conn(tmp_db, project):
    async with project.get_db_connection(database=tmp_db) as conn:
        await project.load_schema(conn=conn)
    assert await project.get_current_version(database=tmp_db) == 4


@pytest.mark.asyncio
async def test_migrate(tmp_db, project):
    await project.migrate(database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 4


@pytest.mark.asyncio
async def test_migrate_bad_file(tmp_db, project):
    project.new_migration("bad_migration", up_content="not valid sql")
    with pytest.raises(asyncpg.PostgresSyntaxError):
        await project.migrate(database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 4


@pytest.mark.asyncio
async def test_rollback(tmp_db, project):
    await project.load_schema(database=tmp_db)
    await project.migrate(target=2, database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 2


@pytest.mark.asyncio
async def test_rollback_bad_file(tmp_db, project):
    project.migrations[2].down.path.write_text("not valid sql")
    await project.load_schema(database=tmp_db)
    with pytest.raises(asyncpg.PostgresSyntaxError):
        await project.migrate(target=2, database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 3


@pytest.mark.asyncio
async def test_rollback_no_down(tmp_db, project):
    await project.load_schema(database=tmp_db)
    with pytest.raises(exceptions.MigrationError) as exc_info:
        await project.migrate(target=1, database=tmp_db)
    assert str(exc_info.value) == (
        "Cannot rollback from version 4 to 1: "
        "one or more migrations do not have down files"
    )


@pytest.mark.asyncio
async def test_rollback_zero(tmp_db, project):
    project.migrations[0].down.path.touch()
    await project.migrate(target=0, database=tmp_db)
    with pytest.raises(exceptions.MigrationError) as exc_info:
        await project.migrate(target=-1, database=tmp_db)
    assert str(exc_info.value) == (
        "Target migration ID '-1' would cause unsupported "
        "rollback of base migration ID '0'"
    )


@pytest.mark.asyncio
async def test_migrate_no_migrations(tmp_db, tmp_path) -> None:
    db = DB.new_project(tmp_path)
    db.migrations.clear()
    await db.migrate(database=tmp_db)
    assert await db.get_current_version(database=tmp_db) is None


@pytest.mark.asyncio
async def test_migrate_no_unapplied_migrations(tmp_db, project):
    await project.load_schema(database=tmp_db)
    await project.migrate(database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 4


@pytest.mark.asyncio
async def test_migrate_different_schema_for_versions(tmp_db, project):
    project.schema_version_table = "schema.table"
    await project.migrate(database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 4


@pytest.mark.asyncio
async def test_migrate_newer_than_migrations(tmp_db, project):
    await project.load_schema(database=tmp_db)
    del project.migrations[4]
    await project.migrate(database=tmp_db)
    assert await project.get_current_version(database=tmp_db) == 4


@pytest.mark.asyncio
async def test_migrate_unknown_target(tmp_db, project):
    target = 10
    with pytest.raises(exceptions.MigrationError) as exc_info:
        await project.migrate(target, database=tmp_db)
    assert (
        str(exc_info.value) == f"Target migration ID '{target}' has no known migration"
    )


@pytest.mark.asyncio
async def test_migrate_rollback_from_unknown_schema(tmp_db, project):
    target = 3
    await project.migrate(target, database=tmp_db)
    del project.migrations[target]
    with pytest.raises(exceptions.MigrationError) as exc_info:
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
async def test_yield_unapplied_migrations_none(tmp_db, tmp_path) -> None:
    db: DB = DB.new_project(tmp_path)
    await db.migrate(database=tmp_db)
    unapplied: list[Migration] = [
        m async for m in db.yield_unapplied_migrations(database=tmp_db)
    ]
    assert len(unapplied) == 0


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
        """,
    )
    output = io.StringIO()
    same = await project.verify(output=output)
    assert not same
    output.seek(0)
    outstr = output.read()
    assert outstr.startswith("--- schema.sql")


def test_load_project_dir(project) -> None:
    expected_migration_count = 5
    project_dir = project.project_dir
    db = DB(project_dir)
    assert len(db.migrations) == expected_migration_count
    assert len(db.fixtures) == 1

    # make sure we can traverse our migration relations
    migrations = []
    child: Union[Migration, None] = db.migrations[0]
    while child:
        migrations.append(child)
        child = child.child

    assert len(migrations) == expected_migration_count


def test_add_fixture_dir(project, extra_fixtures) -> None:
    project.add_fixture_dir(extra_fixtures)

    assert len(project.fixtures) == 2
    for fixture in project.fixtures.values():
        assert fixture.path.parent == extra_fixtures
