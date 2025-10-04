import asyncio

async def waiter(event):
    print("Waiting for event")
    await event.wait()
    print("Event received")

async def setter(event):
    print("doing something")
    await asyncio.sleep(2)
    print("Setting event")
    event.set()

async def main():
    event = asyncio.Event()
    asyncio.create_task(waiter(event))
    await setter(event)

asyncio.run(main())