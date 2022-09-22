import asyncio
from asyncio import StreamReader
from asyncio import StreamWriter

users = {}


async def broadcast(message: str, exclude_username: str):
    writer_coroutines = []
    for username, writer in users.items():
        if exclude_username == username:
            continue

        writer.write(message.encode())
        writer_coroutines.append(writer.drain())

    return await asyncio.gather(*writer_coroutines)


async def handle_echo(reader: StreamReader, writer: StreamWriter):
    async def write(string: str):
        writer.write(string.encode())
        return await writer.drain()

    async def readline() -> str:
        data = await reader.readline()
        return data.decode('utf-8').strip()

    await write('Welcome to budgetchat! What shall I call you?\n')

    nickname = await readline()
    if not nickname or not nickname.isalnum() or nickname in users:
        await write("Invalid or taken username\n")
        writer.write_eof()
        writer.close()
        return

    await asyncio.gather(
        write(f'* The room contains: {", ".join(users.keys())}\n'),
        broadcast(f'* {nickname} has joined the chat\n', exclude_username=nickname)
    )

    users[nickname] = writer
    while not reader.at_eof():
        chatline = await readline()
        if not chatline:
            continue

        await broadcast(f'[{nickname}] {chatline}\n', exclude_username=nickname)

    del users[nickname]
    await broadcast(f'* {nickname} has left the room\n', exclude_username=nickname)


async def main():
    server = await asyncio.start_server(handle_echo, '0.0.0.0', 8888)

    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
