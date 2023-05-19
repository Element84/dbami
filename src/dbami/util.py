import asyncio
import random
import string
from typing import Any, Coroutine


def syncrun(coroutine: Coroutine) -> Any:
    return asyncio.run(coroutine)


def random_name(prefix: str, separator: str = "_") -> str:
    postfix: str = "".join(random.choices(string.ascii_letters, k=5))
    return f"{prefix}{separator}{postfix.lower()}"
