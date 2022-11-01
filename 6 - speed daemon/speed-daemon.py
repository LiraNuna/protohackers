from __future__ import annotations

import asyncio
import bisect
import struct
import traceback
from abc import ABC
from abc import abstractmethod
from asyncio import CancelledError
from asyncio import IncompleteReadError
from asyncio import StreamReader
from asyncio import StreamWriter
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional
from typing import Self


async def read_u8(reader: StreamReader) -> int:
    content = await reader.readexactly(1)
    u8, = struct.unpack('!B', content)
    return u8


async def read_u16(reader: StreamReader) -> int:
    content = await reader.readexactly(2)
    u16, = struct.unpack('!H', content)
    return u16


async def read_u32(reader: StreamReader) -> int:
    content = await reader.readexactly(4)
    u32, = struct.unpack('!I', content)
    return u32


async def read_str(reader: StreamReader) -> str:
    length = await read_u8(reader)
    if not length:
        return ''

    content = await reader.readexactly(length)
    return content.decode('ascii')


async def write_u8(writer: StreamWriter, u8: int) -> None:
    writer.write(u8.to_bytes(length=1, byteorder='big'))
    await writer.drain()


async def write_u16(writer: StreamWriter, u16: int) -> None:
    writer.write(u16.to_bytes(length=2, byteorder='big'))
    await writer.drain()


async def write_u32(writer: StreamWriter, u32: int) -> None:
    writer.write(u32.to_bytes(length=4, byteorder='big'))
    await writer.drain()


async def write_str(writer: StreamWriter, string: str) -> None:
    await write_u8(writer, len(string))
    writer.write(string.encode('ascii'))
    await writer.drain()


@dataclass(frozen=True)
class Message(ABC):
    client: Client

    @staticmethod
    async def read(client: Client) -> Optional[Self]:
        message_type = await read_u8(client.reader)
        match message_type:
            case 0x20:
                return await Plate.parse_body(client)
            case 0x40:
                return await WantHeartbeat.parse_body(client)
            case 0x80:
                return await IAmCamera.parse_body(client)
            case 0x81:
                return await IAmDispatcher.parse_body(client)
            case default:
                raise ClientError('invalid message')

    @staticmethod
    @abstractmethod
    async def parse_body(reader: StreamReader) -> Self:
        raise NotImplementedError

    @abstractmethod
    async def process(self) -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class Plate(Message):
    plate: str
    timestamp: int

    @staticmethod
    async def parse_body(client: Client) -> Self:
        plate = await read_str(client.reader)
        timestamp = await read_u32(client.reader)
        return Plate(client, plate, timestamp)

    async def process_pair(self, sighting1: Sighting, sighting2: Sighting) -> None:
        car_distance = abs(sighting2.camera.mile - sighting1.camera.mile)
        elapsed_time = sighting2.plate.timestamp - sighting1.plate.timestamp
        speed_mph = car_distance / (elapsed_time / 3600)
        if sighting2.camera.limit >= round(speed_mph):
            return

        ticket = Ticket(
            sighting1.plate.plate,
            sighting1.camera.road,
            sighting1.camera.mile,
            sighting1.plate.timestamp,
            sighting2.camera.mile,
            sighting2.plate.timestamp,
            speed_mph,
        )
        if sighting1.camera.road not in dispatchers:
            queued_tickets[ticket.road].append(ticket)
        else:
            await dispatchers[sighting1.camera.road].send_ticket(ticket)

    async def process(self) -> None:
        if not isinstance(self.client.type, IAmCamera):
            raise ClientError('not a camera')

        sighting = Sighting(self, self.client.type)
        sightings = plate_sightings[sighting.plate.plate]
        bisect.insort_left(sightings, sighting, key=lambda s: s.plate.timestamp)
        if len(sightings) < 2:
            return

        to_process = []
        for sighting1, sighting2 in zip(sightings, sightings[1:]):
            to_process.append(self.process_pair(sighting1, sighting2))

        await asyncio.gather(*to_process)


@dataclass(frozen=True)
class WantHeartbeat(Message):
    interval: int

    @staticmethod
    async def parse_body(client: Client) -> Self:
        interval = await read_u32(client.reader)
        return WantHeartbeat(client, interval)

    async def heartbeat_task(self):
        try:
            while not self.client.reader.at_eof():
                await asyncio.sleep(self.interval / 10)
                await write_u8(self.client.writer, 0x41)
        except ConnectionResetError:
            pass

    async def process(self) -> None:
        if self.client.has_heartbeat:
            raise ClientError('already has a heartbeat')

        self.client.has_heartbeat = True
        if self.interval:
            asyncio.create_task(self.heartbeat_task())


@dataclass(frozen=True)
class IAmCamera(Message):
    road: int
    mile: int
    limit: int

    @staticmethod
    async def parse_body(client: Client) -> Self:
        road = await read_u16(client.reader)
        mile = await read_u16(client.reader)
        limit = await read_u16(client.reader)
        return IAmCamera(client, road, mile, limit)

    async def process(self) -> None:
        await self.client.identify(self)


@dataclass(frozen=True)
class IAmDispatcher(Message):
    roads: list[int]

    @staticmethod
    async def parse_body(client: Client) -> Self:
        num_roads = await read_u8(client.reader)

        roads = []
        for _ in range(num_roads):
            roads.append(await read_u16(client.reader))

        return IAmDispatcher(client, roads)

    async def process(self) -> None:
        await self.client.identify(self)

        for road in self.roads:
            dispatchers[road] = self.client

            queued_tickets_for_road = queued_tickets[road]
            await asyncio.gather(*(self.client.send_ticket(ticket) for ticket in queued_tickets_for_road))
            queued_tickets[road] = []


@dataclass(frozen=True)
class ClientError(Exception):
    message: str


@dataclass(frozen=True)
class Sighting:
    plate: Plate
    camera: IAmCamera


@dataclass(frozen=True)
class Ticket:
    plate: str
    road: int
    mile_from: int
    timestamp1: int
    mile_to: int
    timestamp2: int
    speed: float


@dataclass
class Client:
    reader: StreamReader
    writer: StreamWriter
    has_heartbeat: bool = False
    type: Optional[IAmCamera | IAmDispatcher] = None

    async def send_ticket(self, ticket: Ticket) -> None:
        days = set(ts // 86400 for ts in (ticket.timestamp1, ticket.timestamp2))
        for sent_ticket in sent_tickets[ticket.plate]:
            if any(ts // 86400 in days for ts in (sent_ticket.timestamp1, sent_ticket.timestamp2)):
                return

        sent_tickets[ticket.plate].add(ticket)

        await write_u8(self.writer, 0x21)
        await write_str(self.writer, ticket.plate)
        await write_u16(self.writer, ticket.road)
        await write_u16(self.writer, ticket.mile_from)
        await write_u32(self.writer, ticket.timestamp1)
        await write_u16(self.writer, ticket.mile_to)
        await write_u32(self.writer, ticket.timestamp2)
        await write_u16(self.writer, int(ticket.speed * 100))

    async def handle_messages(self):
        while not self.reader.at_eof():
            try:
                message = await Message.read(self)
                await message.process()
            except ClientError as e:
                await write_u8(self.writer, 0x10)
                await write_str(self.writer, e.message)

    async def identify(self, type: IAmCamera | IAmDispatcher):
        if self.type:
            raise ClientError('already identified')

        self.type = type


dispatchers: dict[int, Client] = {}
queued_tickets: dict[int, list[Ticket]] = defaultdict(list)
plate_sightings: dict[str, list[Sighting]] = defaultdict(list)
sent_tickets: dict[str, set[Ticket]] = defaultdict(set)


async def handle_client(reader: StreamReader, writer: StreamWriter) -> None:
    client = Client(reader, writer)
    try:
        await client.handle_messages()
    except (CancelledError, IncompleteReadError, KeyboardInterrupt):
        pass
    except:
        traceback.print_exc()


async def main():
    server = await asyncio.start_server(handle_client, '0.0.0.0', 8888)
    print("Accepting connections...")

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
