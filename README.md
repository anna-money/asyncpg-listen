# asyncpg-listen

This library simplifies usage of listen/notify with [asyncpg](https://github.com/MagicStack/asyncpg):
1. Handles loss of a connection
2. Simplifies notifications processing from multiple channels
3. Setups a timeout for receiving a notification
4. Allows to receive all notifications/only last notification depending on ListenPolicy.

```python
import asyncio
import asyncpg
import asyncpg_listen


async def handle_notifications(notification: asyncpg_listen.NotificationOrTimeout) -> None:
    print(f"{notification} has been received")


async def main():
    listener = asyncpg_listen.NotificationListener(asyncpg_listen.connect_func(user="postgres"))
    listener_task = asyncio.create_task(
        listener.run(
            {"simple": handle_notifications},
            policy=asyncpg_listen.ListenPolicy.LAST,
            notification_timeout=5
        )
    )

    await asyncio.sleep(1)

    connection = await asyncpg.connect(user="postgres")
    try:
        for i in range(42):
            await connection.execute(f"NOTIFY simple, '{i}'")
    finally:
        await connection.close()

    await asyncio.sleep(1)

    listener_task.cancel()


asyncio.run(main())

```
