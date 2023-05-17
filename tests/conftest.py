import random
import string
from pathlib import Path

import pytest

from dbami.db import DB
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
