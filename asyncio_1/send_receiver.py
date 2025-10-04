import asyncio

async def send(q):
    for i in range(3):
        await q.put(f"Msg {i}")
        print(f"Sent {i}")
        await asyncio.sleep(1)

async def receive(q):
    while True:
        msg = await q.get()
        print(f"Received {msg}")

async def main():
    q = asyncio.Queue()
    asyncio.create_task(send(q))
    asyncio.create_task(receive(q))
    await asyncio.sleep(5)  

asyncio.run(main())
