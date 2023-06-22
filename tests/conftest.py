import os
from pathlib import Path

import asyncpg
import pytest

from dbami.db import DB
from dbami.util import syncrun


@pytest.fixture(scope="session")
def test_db_name_stem() -> str:
    return "dbami_test"


@pytest.fixture
def tmp_chdir(tmp_path: Path):
    old = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(old)


@pytest.fixture
def empty_project(tmp_chdir: Path):
    db: DB = DB.new_project(tmp_chdir)
    return db


@pytest.fixture
def project(tmp_chdir: Path):
    # create some migrations before instantiating the DB instance
    # to ensure we test the migration load process
    migrations_dir = DB.project_migrations(tmp_chdir)
    migrations_dir.mkdir()
    migrations_dir.joinpath("00000_base.down.sql").touch()
    migrations_dir.joinpath("01_migration.up.sql").touch()
    db: DB = DB.new_project(tmp_chdir)
    db.new_migration("migration")
    db.new_migration("migration")
    db.new_migration("migration")
    db.new_fixture("a_fixture")
    db.fixtures["a_fixture"].path.write_text("select current_database();")
    return db


@pytest.fixture
def extra_fixtures(tmp_path: Path) -> Path:
    tfdir = tmp_path.joinpath("test_fixtures")
    tfdir.mkdir()
    # override the fixture in the project fixtures dir
    tfdir.joinpath("a_fixture.sql").write_text("select current_database();")
    # add another fixture
    tfdir.joinpath("b_fixture.sql").write_text("select current_database();")
    return tfdir


@pytest.fixture
def tmp_db_name(test_db_name_stem: str, request):
    db_name = (
        f"{test_db_name_stem}_{request.module.__name__}:{request.function.__name__}"
    )

    try:
        yield db_name
    finally:
        try:
            syncrun(DB.drop_database(db_name))
        except asyncpg.InvalidCatalogNameError:
            pass


@pytest.fixture
def tmp_db(tmp_db_name: str):
    syncrun(DB.create_database(tmp_db_name))
    return tmp_db_name
