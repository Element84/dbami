import pytest

from dbami.pg_dump import pg_dump

EMPTY_DUMP: str = """--
-- PostgreSQL database dump
--


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


def remove_versions(dump: str):
    return "\n".join(
        [
            line
            for line in dump.splitlines()
            if not (
                line.startswith("-- Dumped from database version")
                or line.startswith("-- Dumped by pg_dump version")
            )
        ]
    )


@pytest.mark.asyncio
async def test_pg_dump(tmp_db) -> None:
    rc, dump = await pg_dump("-d", tmp_db)
    assert rc == 0
    assert remove_versions(dump) == EMPTY_DUMP


@pytest.mark.asyncio
async def test_pg_dump_custom_path(tmp_db) -> None:
    rc, dump = await pg_dump(
        "compose",
        "exec",
        "postgres",
        "pg_dump",
        "-U",
        "postgres",
        "-d",
        tmp_db,
        pg_dump="docker",
    )
    assert rc == 0
    assert remove_versions(dump) == EMPTY_DUMP


@pytest.mark.asyncio
async def test_pg_dump_bad_path(tmp_db) -> None:
    with pytest.raises(FileNotFoundError) as exc_info:
        await pg_dump(
            "-d",
            tmp_db,
            pg_dump="/something/that/does/not/exist",
        )
    assert str(exc_info.value).startswith("pg_dump could not be located: ")
