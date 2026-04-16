import asyncio
import logging

logger = logging.getLogger("chainpulse")

_task_registry = set()


async def run_task(name: str, coro):
    """
    Runs a coroutine as a tracked background task.
    Prevents garbage collection and logs failures.
    """
    task = asyncio.create_task(coro, name=name)
    _task_registry.add(task)

    def on_done(t):
        _task_registry.discard(t)
        if t.cancelled():
            logger.warning(f"Task {name} was cancelled")
        elif t.exception():
            logger.error(f"Task {name} failed: {t.exception()}")
        else:
            logger.info(f"Task {name} completed")

    task.add_done_callback(on_done)
    return task


def get_running_tasks() -> list:
    return [
        {"name": t.get_name(), "done": t.done()}
        for t in _task_registry
    ]
