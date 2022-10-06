import asyncio
from asyncio import DatagramTransport

database: dict[bytes, bytes] = {}


class KenKeyValueProtocol:
    transport: DatagramTransport

    def connection_made(self, transport: DatagramTransport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        key, is_insert, value = data.partition(b'=')
        if key == b'version':
            return self.transport.sendto(b"version=Ken's Key-Value Store 1.0", addr)
        if not is_insert:
            return self.transport.sendto(key + b'=' + database.get(key, b''), addr)
        if is_insert:
            database[key] = value


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    listen = loop.create_datagram_endpoint(KenKeyValueProtocol, local_addr=('0.0.0.0', 8888))
    transport, protocol = loop.run_until_complete(listen)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    transport.close()
    loop.close()
