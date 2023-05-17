import asyncio


def syncrun(coroutine):
    return asyncio.run(coroutine)
