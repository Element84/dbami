import asyncio
import os
from pathlib import Path
from typing import Optional, Union


async def pg_dump(
    *args, pg_dump: Union[str, Path, None] = None
) -> tuple[Optional[int], str]:
    """Async wrapper for executing pg_dump

    Requires that pg_dump be on the path, or its path provided via the pg_dump kwarg.

    Args:
      *args:
        All arguments are forwared to pg_dump. See its docs for what it supports.

    Keyword Args:
      pg_dump: str | Path | None
        Path to or name of the pg_dump command (default "pg_dump")
    """
    if pg_dump is None:
        pg_dump = os.getenv("DBAMI_PG_DUMP", "pg_dump")

    try:
        proc = await asyncio.create_subprocess_exec(
            pg_dump,
            *args,
            stdout=asyncio.subprocess.PIPE,
            # this just goes to the parent's stderr
            stderr=None,
            # no need for stdin, don't let it consume ours
            stdin=asyncio.subprocess.DEVNULL,
        )
    except FileNotFoundError:
        raise FileNotFoundError(f"pg_dump could not be located: '{pg_dump}'")

    stdout, _ = await proc.communicate()

    return (proc.returncode, stdout.decode())
