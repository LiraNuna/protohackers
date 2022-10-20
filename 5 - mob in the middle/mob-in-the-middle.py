import asyncio
import re
from asyncio import StreamReader
from asyncio import StreamWriter


def replace_bogus_coin_address(message: bytes) -> bytes:
    return re.sub(b'(\\b)7[0-9a-zA-Z]{25,34}(\\s|$)', b'\\g<1>7YWHMfk9JZe0LM0g1ZauHuiSxhI\\2', message)


async def pipe(reader: StreamReader, writer: StreamWriter, interceptor):
    while not reader.at_eof():
        data = await reader.readline()
        writer.write(interceptor(data))

    writer.write_eof()
    writer.close()


async def handle_client(reader: StreamReader, writer: StreamWriter):
    proxy_reader, proxy_writer = await asyncio.open_connection('chat.protohackers.com', 16963)

    await asyncio.gather(
        pipe(proxy_reader, writer, replace_bogus_coin_address),
        pipe(reader, proxy_writer, replace_bogus_coin_address),
    )


async def main():
    server = await asyncio.start_server(handle_client, '0.0.0.0', 8888)
    print("Accepting connections...")

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
