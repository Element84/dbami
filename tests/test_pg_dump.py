import pytest

from dbami.db import pg_dump


@pytest.mark.asyncio
async def test_dump(tmp_db) -> None:
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
    rc, dump = await pg_dump("-d", tmp_db)
    assert rc == 0
    assert dump == empty_dump
