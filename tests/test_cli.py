import contextlib
import io
import sys
from pathlib import Path
from typing import Optional, TextIO

import asyncpg
import pytest

from dbami import exceptions
from dbami.__main__ import main as cli_main
from dbami.db import DB
from dbami.util import syncrun


@contextlib.contextmanager
def replace_streams(stdin: Optional[TextIO] = None):
    new_in = io.StringIO() if stdin is None else stdin
    new_out, new_err = io.StringIO(), io.StringIO()
    old_in, old_out, old_err = sys.stdin, sys.stdout, sys.stderr
    try:
        sys.stdin, sys.stdout, sys.stderr = new_in, new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdin, sys.stdout, sys.stderr = old_in, old_out, old_err


def run_cli(*args, stdin: Optional[TextIO] = None):
    rc = None
    with replace_streams(stdin=stdin) as (out, err):
        try:
            cli_main(args)
        except SystemExit as e:
            rc = e.code

    out.seek(0)
    err.seek(0)

    return rc, out.read(), err.read()


async def database_exists(dbname) -> bool:
    try:
        async with DB.get_db_connection(database=dbname):
            return True
    except asyncpg.InvalidCatalogNameError:
        return False


@pytest.fixture
def project_dir(project: DB) -> Path:
    return project.project_dir


def test_cli():
    rc, out, err = run_cli()
    print(out)
    print(err)
    assert rc == 2


def test_init(tmp_chdir):
    rc, out, err = run_cli("init")
    print(out)
    print(err)
    assert rc == 0
    DB(tmp_chdir)  # constructor runs validate
    assert True


def test_new(project_dir):
    rc, out, err = run_cli("new", "a_migration")
    print(out)
    print(err)
    assert rc == 0
    assert (
        len(list(project_dir.joinpath("migrations").glob("*_a_migration.*.sql"))) == 2
    )


def test_new_no_name(project_dir):
    rc, out, err = run_cli("new")
    print(out)
    print(err)
    assert rc == 2


def test_create(tmp_db_name):
    rc, out, err = run_cli("create", "--database", tmp_db_name)
    print(out)
    print(err)
    assert rc == 0

    assert syncrun(database_exists(tmp_db_name))


def test_create_twice(tmp_db_name):
    rc, out, err = run_cli("create", "--database", tmp_db_name)
    print(out)
    print(err)
    assert rc == 0

    with pytest.raises(asyncpg.DuplicateDatabaseError):
        run_cli("create", "--database", tmp_db_name)


def test_drop(tmp_db_name):
    rc, out, err = run_cli("create", "--database", tmp_db_name)
    print(out)
    print(err)
    assert rc == 0

    assert syncrun(database_exists(tmp_db_name))

    rc, out, err = run_cli("drop", "--database", tmp_db_name)
    print(out)
    print(err)
    assert rc == 0

    assert not syncrun(database_exists(tmp_db_name))


def test_drop_no_exist(tmp_db_name):
    with pytest.raises(asyncpg.InvalidCatalogNameError):
        run_cli("drop", "--database", tmp_db_name)


def test_pending(tmp_db, project_dir):
    rc, out, err = run_cli("pending", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0
    assert len(out.splitlines()) == 5


def test_current_schema(tmp_db, project_dir):
    rc, out, err = run_cli("current-schema", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0
    assert out == "None\n"


def test_load_schema(tmp_db, project_dir):
    rc, out, err = run_cli("load-schema", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0


def test_migrate(tmp_db, project_dir):
    rc, out, err = run_cli("migrate", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("current-schema", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0
    assert out == "4\n"


def test_migrate_specific_target(tmp_db, project_dir):
    target = 2
    rc, out, err = run_cli("migrate", "--target", str(target), "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("current-schema", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0
    assert out == f"{target}\n"


def test_migrate_noop(tmp_db, project_dir):
    rc, out, err = run_cli("migrate", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("migrate", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0


def test_migrate_different_version_table(tmp_db, project_dir):
    try:
        rc, out, err = run_cli(
            "migrate",
            "--database",
            tmp_db,
            "--schema-version-table",
            "aschema.table",
        )
    except Exception as e:
        print(e)
    else:
        print(out)
        print(err)
        assert rc == 0

    rc, out, err = run_cli(
        "current-schema",
        "--database",
        tmp_db,
        "--schema-version-table",
        "aschema.table",
    )
    print(out)
    print(err)
    assert rc == 0
    assert out == "4\n"


def test_migrate_bad_target(tmp_db, project_dir):
    target = 10
    with pytest.raises(exceptions.MigrationError) as exc_info:
        run_cli("migrate", "--target", str(target), "--database", tmp_db)
    assert (
        str(exc_info.value) == f"Target migration ID '{target}' has no known migration"
    )


def test_migrate_wrong_direction(tmp_db, project_dir):
    rc, out, err = run_cli("migrate", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("migrate", "--target", "0", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 1
    assert (
        err == "Target would roll back version and direction is up: can't go 4 -> 0\n"
    )


def test_rollback(tmp_db, project_dir):
    rc, out, err = run_cli("migrate", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("rollback", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("current-schema", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0
    assert out == "3\n"


def test_rollback_specific_target(tmp_db, project_dir):
    rc, out, err = run_cli("migrate", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    target = 3
    rc, out, err = run_cli("rollback", "--target", str(target), "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("current-schema", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0
    assert out == f"{target}\n"


def test_rollback_noop(tmp_db, project_dir):
    target = 0
    rc, out, err = run_cli("migrate", "--target", str(target), "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("current-schema", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0
    assert out == f"{target}\n"

    rc, out, err = run_cli("rollback", "--target", str(target), "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("current-schema", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0
    assert out == f"{target}\n"


def test_rollback_bad_target(tmp_db, project_dir):
    rc, out, err = run_cli("migrate", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    target = 5
    rc, out, err = run_cli("rollback", "--target", str(target), "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 1
    assert err == f"Target migration ID '{target}' has no known migration\n"


def test_rollback_nonint_target(tmp_db, project_dir):
    target = "not-an-int"
    rc, out, err = run_cli("rollback", "--target", str(target), "--database", tmp_db)
    assert rc != 0


def test_rollback_wrong_direction(tmp_db, project_dir):
    rc, out, err = run_cli("migrate", "--target", "2", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("rollback", "--target", "4", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 1
    assert (
        err
        == "Target would move version forward and direction is down: can't go 2 -> 4\n"
    )


def test_rollback_zero(tmp_db, project_dir):
    DB(project_dir).migrations[0].down.path.touch()
    rc, out, err = run_cli("migrate", "--target", "0", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("rollback", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 1
    assert err == (
        "Target migration ID '-1' would cause unsupported "
        "rollback of base migration ID '0'\n"
    )


def test_rollback_no_schema(tmp_db, project_dir):
    rc, out, err = run_cli("rollback", "--database", tmp_db)
    print(out)
    print(err)
    assert rc == 1
    assert err == "Cannot rollback: database has no applied schema version\n"


def test_up(tmp_db_name, project_dir):
    rc, out, err = run_cli(
        "up",
        "--database",
        tmp_db_name,
        "--schema-version-table",
        "schema.table",
    )
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli(
        "current-schema",
        "--database",
        tmp_db_name,
        "--schema-version-table",
        "schema.table",
    )
    print(out)
    print(err)
    assert rc == 0
    assert out == "4\n"


def test_up_twice(tmp_db_name, project_dir):
    rc, out, err = run_cli("up", "--database", tmp_db_name)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("up", "--database", tmp_db_name)
    print(out)
    print(err)
    assert rc == 0

    rc, out, err = run_cli("current-schema", "--database", tmp_db_name)
    print(out)
    print(err)
    assert rc == 0
    assert out == "4\n"


def test_verify(tmp_db, project_dir):
    rc, out, err = run_cli("verify")
    print(out)
    print(err)
    assert rc == 0


def test_verify_bad_pg_dump(tmp_db, project_dir):
    with pytest.raises(FileNotFoundError) as exc_info:
        run_cli("verify", "--pg-dump", "/bad/path")
    assert str(exc_info.value).startswith("pg_dump could not be located:")


def test_verify_schema_diff(tmp_db, project_dir):
    DB(project_dir).migrations[4].up.path.write_text(
        "CREATE TABLE test_table (id text NOT NULL);"
    )
    rc, out, err = run_cli("verify")
    print(out)
    print(err)
    assert rc == 1
    assert err.startswith("--- schema.sql")


def test_version():
    from dbami.version import __version__

    rc, out, err = run_cli("version")
    print(out)
    print(err)
    assert rc == 0
    assert out.strip().endswith(str(__version__))


def test_list_fixtures(project_dir):
    rc, out, err = run_cli("list-fixtures")
    print(out)
    print(err)
    assert rc == 0
    assert len(out.splitlines()) == 1
    assert out.startswith("a_fixture (")


def test_list_fixtures_extra(project_dir, extra_fixtures):
    rc, out, err = run_cli("list-fixtures", "--fixture-dir", str(extra_fixtures))
    print(out)
    print(err)
    assert rc == 0
    assert len(out.splitlines()) == 2


def test_load_fixture(tmp_db, project_dir):
    rc, out, err = run_cli("load-fixture", "--database", tmp_db, "a_fixture")
    print(out)
    print(err)
    assert rc == 0


def test_load_fixtures_extra(tmp_db, project_dir, extra_fixtures):
    rc, out, err = run_cli(
        "load-fixture",
        "--database",
        tmp_db,
        "--fixture-dir",
        str(extra_fixtures),
        "b_fixture",
    )
    print(out)
    print(err)
    assert rc == 0


def test_execute_sql(tmp_db, project):
    stdin = io.StringIO()
    stdin.write("create table a_table (id int primary key);")
    stdin.seek(0)
    rc, out, err = run_cli(
        "execute-sql",
        "--database",
        tmp_db,
        stdin=stdin,
    )
    print(out)
    print(err)
    assert rc == 0
    syncrun(project.execute_sql("select * from a_table", database=tmp_db))
    assert True
