import asyncio
import json
import math
from asyncio import IncompleteReadError
from asyncio import StreamReader
from asyncio import StreamWriter
from typing import Any
from typing import Optional
from typing import Union


def is_prime(number: Union[int, float]) -> bool:
    # 0 and negative numbers are not prime
    if number < 2:
        return False
    # according to spec, non-integers cannot be prime
    if isinstance(number, float):
        return False

    for i in range(2, int(math.sqrt(number)) + 1):
        if (number % i) == 0:
            return False

    return True


def get_valid_request(request_json: str) -> Optional[dict[str, Any]]:
    try:
        request = json.loads(request_json)
    except json.JSONDecodeError:
        return None

    if request.get('method', None) != 'isPrime':
        return None

    number = request.get('number', None)
    # In python, booleans are instanceof int for historical reasons.
    if isinstance(number, bool):
        return None
    if not isinstance(number, (int, float)):
        return None

    return request


async def handle_is_prime(reader: StreamReader, writer: StreamWriter) -> None:
    async def send(response: bytes) -> None:
        writer.write(response + b'\n')
        await writer.drain()

    async def end_connection() -> None:
        await send(b'{"error": "malformed request"}')
        writer.close()

    try:
        while data := await reader.readuntil(separator=b'\n'):
            request = get_valid_request(data.decode('ascii'))
            if not request:
                return await end_connection()

            await send(json.dumps({
                'method': 'isPrime',
                'prime': is_prime(request['number']),
            }).encode('ascii'))

        writer.close()
    except IncompleteReadError:
        writer.close()


async def main():
    server = await asyncio.start_server(handle_is_prime, '0.0.0.0', 8888)
    print("Accepting connections...")

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
