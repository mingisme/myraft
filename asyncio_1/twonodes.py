
import asyncio


async def node(name):
    for i in range(5):
        print(f"{name} running {i}")
        await asyncio.sleep(1)  # non-blocking sleep


async def main():
        asyncio.create_task(node("A"))
        asyncio.create_task(node("B"))
        await asyncio.sleep(6)  # wait for tasks        

asyncio.run(main())        