import asyncio
import bisect
import statistics
import struct
from asyncio import IncompleteReadError
from asyncio import StreamReader
from asyncio import StreamWriter
from statistics import StatisticsError
from typing import Iterable
from typing import NamedTuple


class InputRecord(NamedTuple):
    type: str
    timestamp: int
    price: int


class QueryRecord(NamedTuple):
    type: str
    mintime: int
    maxtime: int


class RequestContext:
    reader: StreamReader
    writer: StreamWriter
    database: list[InputRecord]
    
    def __init__(self, reader: StreamReader, writer: StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self.database = []
    
    def insert(self, record: InputRecord) -> None:
        bisect.insort_left(self.database, record, key=lambda r: r.timestamp)
    
    def get_range(self, start: int, end: int) -> Iterable[int]:
        return (
            record.price
            for record in self.database[start:end]
        )


def handle_output_record(record: QueryRecord, context: RequestContext) -> None:
    try:
        start_index = bisect.bisect_left(context.database, record.mintime, key=lambda r: r.timestamp)
        end_index = bisect.bisect_right(context.database, record.maxtime, key=lambda r: r.timestamp)
        mean = int(statistics.mean(context.get_range(start_index, end_index)))
    except StatisticsError:
        mean = 0
    
    context.writer.write(mean.to_bytes(4, byteorder='big', signed=True))


def handle_record(record: bytes, context: RequestContext) -> None:
    record_type, int1, int2 = struct.unpack('!cii', record)
    print(">>>>>>", record_type, int1, int2)
    if record_type == b'I':
        return context.insert(InputRecord(record_type, int1, int2))
    if record_type == b'Q':
        return handle_output_record(QueryRecord(record_type, int1, int2), context)
    
    raise ValueError(f'Unknown record type: {record_type}')


async def handle_request(reader: StreamReader, writer: StreamWriter) -> None:
    try:
        context = RequestContext(reader, writer)
        while record := await reader.readexactly(9):
            handle_record(record, context)
        
        writer.write_eof()
        writer.close()
    except (IncompleteReadError, ValueError) as e:
        print(e)
        
        writer.write_eof()
        writer.close()


async def main():
    server = await asyncio.start_server(handle_request, '0.0.0.0', 8888)
    print("Accepting connections...")
    
    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
