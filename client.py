import asyncio
import time


class ClientError(Exception):
    pass


class Client(object):

    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.__loop = asyncio.get_event_loop()

    def get(self, key, *args, **kwargs):
        try:
            task = asyncio.wait_for(
                self.__get(key, *args, **kwargs),
                timeout=self.timeout
            )
            self.__loop.run_until_complete(task)
        except asyncio.TimeoutError as e:
            print(e)
        except Exception as e:
            print(e)

    def put(self, key, value, timestamp=None, *args, **kwargs):
        try:
            task = asyncio.wait_for(
                self.__put(key, value, timestamp, *args, **kwargs),
                timeout=self.timeout
            )
            self.__loop.run_until_complete(task)
        except asyncio.TimeoutError as e:
            print(e)
        except Exception as e:
            print(e)

    async def __get(self, key, *args, **kwargs):
        reader, writer = await asyncio.open_connection(
            self.host,
            self.port,
            loop=self.__loop
        )

        writer.write(bytes('get ' + str(key), 'utf-8'))

        await writer.drain()

        while True:
            data = await reader.read(1024)
            if not data:
                break
            print(data)
        writer.close()

    async def __put(self, key, value, timestamp, *args, **kwargs):
        reader, writer = await asyncio.open_connection(
            self.host,
            self.port,
            loop=self.__loop
        )
        timestamp = timestamp or int(time.time())
        writer.write(
            bytes(
                "put" + " " + str(key) + " " + str(value) + " " + str(timestamp),
                'utf-8'
            )
        )
        await writer.drain()

        data = await reader.read(1024)
        print(data.decode('utf-8'))
        writer.close()
