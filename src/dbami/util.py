import asyncio
import random
import string


def syncrun(coroutine):
    return asyncio.run(coroutine)


def random_name(prefix: str, separator: str = "_") -> str:
    postfix: str = "".join(random.choices(string.ascii_letters, k=5))
    return f"{prefix}{separator}{postfix.lower()}"
