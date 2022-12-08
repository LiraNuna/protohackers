import asyncio
import re
from asyncio import DatagramTransport
from dataclasses import dataclass
from dataclasses import field
from itertools import islice


@dataclass
class Session:
    id: bytes
    transport: DatagramTransport
    addr: tuple[str, int]
    data: list[bytes] = field(default_factory=list)
    cursor: int = 0
    send_cursor: int = 0
    acked_until: int = 0
    is_closed: bool = False
    send_buffer: list[bytes] = field(default_factory=list)

    def add_data(self, pos: int, data: list[bytes]) -> None:
        new_max_size = pos + len(data)
        self.data += (new_max_size - len(self.data)) * [b'']

        for i in range(pos, pos + len(data)):
            self.data[i] = data[i - pos]

    def handle_data(self) -> None:
        if b'' in self.data:
            return
        for i in range(self.cursor, len(self.data)):
            if self.data[i] == b'\n':
                line = self.data[self.cursor:i]

                self.send_buffer += reversed(line)
                self.send_buffer += [b'\n']
                self.send_data()

                self.cursor = i + 1
                self.send_cursor += len(line) + 1

    def send_data(self):
        if self.is_closed:
            return

        CHUNK_SIZE = 512
        cursor = 0
        it = iter(self.send_buffer)
        for slice in iter(lambda: tuple(islice(it, CHUNK_SIZE)), ()):
            to_send = b'/data/' + self.id + b'/' + str(cursor).encode() + b'/' + bytes(b''.join(slice)) + b'/'
            self.transport.sendto(to_send, self.addr)
            cursor += CHUNK_SIZE


sessions: dict[bytes, Session] = {}


def maybe_retransmit():
    for session in sessions.values():
        if session.is_closed:
            continue
        # if session.send_cursor != session.acked_until:
        session.send_data()


async def retransmit_task():
    while True:
        maybe_retransmit()
        await asyncio.sleep(3)


class LRCP:
    transport: DatagramTransport

    def connection_made(self, transport: DatagramTransport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        if data[0] != 47 or data[-1] != 47:
            return

        msg_type, rest = data[1:-1].split(b'/', 1)

        if msg_type == b'connect':
            session_id, = rest.split(b'/', 1)
            if int(session_id) > 2 ** 31:
                return
            if session_id not in sessions:
                sessions[session_id] = Session(session_id, transport, addr)
            self.transport.sendto(b'/ack/' + session_id + b'/0/', addr)
        if msg_type == b'data':
            session_id, pos, data = rest.split(b'/', 2)
            if session_id not in sessions or sessions[session_id].is_closed:
                return self.transport.sendto(b'/close/' + session_id + b'/', addr)

            split_data = [x for x in re.split(br'(\\?.)', data) if x]
            if b'/' in split_data:
                return

            sessions[session_id].add_data(int(pos), split_data)
            sessions[session_id].handle_data()
            try:
                return self.transport.sendto(
                    b'/ack/' + session_id + b'/' + str(sessions[session_id].data.index(b'')).encode() + b'/',
                    addr)
            except ValueError:
                return self.transport.sendto(
                    b'/ack/' + session_id + b'/' + str(len(sessions[session_id].data)).encode() + b'/',
                    addr)

        if msg_type == b'close':
            session_id, = rest.split(b'/', 1)
            session = sessions[session_id]
            session.is_closed = True
            session.handle_data()
            session.transport.sendto(b'/close/' + session_id + b'/', addr)

        if msg_type == b'ack':
            session_id, until = rest.split(b'/', 2)
            session = sessions[session_id]
            session.acked_until = int(until)
            if session.acked_until > len(session.data):
                session.transport.sendto(b'/close/' + session_id + b'/', addr)


class FakeTransport:
    def sendto(self, data, addr):
        pass


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    listen = loop.create_datagram_endpoint(LRCP, local_addr=('0.0.0.0', 8888))
    transport, protocol = loop.run_until_complete(listen)

    try:
        loop.create_task(retransmit_task())
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    transport.close()
    loop.close()
