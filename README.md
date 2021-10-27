# asyncpg-listen

This library simplifies usage of listen/notify with [asyncpg](https://github.com/MagicStack/asyncpg):
1. Handles lost of a connection
1. Simplifies processing notifications from multiple channels
1. Setups a timeout for receiving a notification
1. Allows to receive all notifications/only last notification depends on `ListenPolicy`.

```python
import asyncio
import asyncpg
import asyncpg_listen


async def handle_notifications(notification: asyncpg_listen.NotificationOrTimeout) -> None:
    print(f"{notification} has been received")


listener = asyncpg_listen.NotificationListener(asyncpg_listen.connect_func())
listener_task = asyncio.create_task(
    listener.run(
        {"channel": handle_notifications},
        policy=asyncpg_listen.ListenPolicy.LAST,
        notification_timeout=1
    )
)

async with asyncpg.connect() as connection:
    for i in range(42):
        await connection.execute(f"NOTIFY simple, '{i}'")
```
