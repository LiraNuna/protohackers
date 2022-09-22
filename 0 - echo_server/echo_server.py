import asyncio
from asyncio import StreamReader
from asyncio import StreamWriter


async def handle_echo(reader: StreamReader, writer: StreamWriter):
    data = await reader.read()

    writer.write(data)
    await writer.drain()

    writer.close()


async def main():
    server = await asyncio.start_server(handle_echo, '0.0.0.0', 8888)
    print("Accepting connections...")

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
