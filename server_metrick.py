import aiomysql
import asyncio
import pymysql
import re
import json


class BackConn(type):

    def __new__(cls, clsname, superclasses, attributedict):
        try:
            loop = asyncio.get_event_loop()
            coro = asyncio.start_server(
                client_connected_cb=attributedict['handler'],
                host=attributedict['host_server'],
                port=attributedict['port_server'],
                loop=loop
            )

            conn = aiomysql.connect(
                host=attributedict['host_db'],
                port=attributedict['port_db'],
                user=attributedict['user_db'],
                password=attributedict['password_db'],
                db=attributedict['db'],
                loop=loop,
                cursorclass=aiomysql.cursors.DictCursor
            )
            conn = loop.run_until_complete(conn)
            server = loop.run_until_complete(coro)

            attributedict['conn'] = conn
            attributedict['loop'] = loop
            attributedict['server'] = server
            return type.__new__(cls, clsname, superclasses, attributedict)
        except Exception as e:
            raise Exception(e)


class BaseServer(metaclass=BackConn):

    host_server = '127.0.0.1'
    port_server = 10001
    host_db = '127.0.0.1'
    port_db = 3306
    password_db = 'metrick user'
    db = 'metrick_db'
    user_db = 'metrick_user'

    async def handler(reader, writer):
        r = await reader.read(1024)
        data = ""
        try:
            data = await BaseServer.handle_input(r.decode())
        except Exception as e:
            print(e)
            data = '\nERROR!\n'
        finally:
            writer.write(data.encode())
            writer.close()

    async def get(key):
        async with BaseServer.conn.cursor() as cur:
            if key == '*':
                await cur.execute("select * from metrick")
            else:
                await cur.execute(
                    """
                    select * from metrick where metrick = \"{key}\"
                    """.format(key=key)
                )
            return await cur.fetchall()

    async def put(key, value, timestamp):
        async with BaseServer.conn.cursor() as cur:
            await cur.execute(
                """
                insert into metrick(metrick, metrick_val, metrick_timestamp)
                values
                (\"{key}\", \"{value}\", \"{timestamp}\")
                """.format(key=key, value=value, timestamp=timestamp)
            )
            await BaseServer.conn.commit()

    async def add_log(log):
        pass

    async def handle_input(n_input):
        get_pat = re.compile('^(get [^\s]{1,15})$')
        put_pat = re.compile('^(put [^\s]{2,15} [^\s]{1,15} [^\s]{2,20})$')
        if get_pat.match(n_input):
            try:
                r = await BaseServer.get(*n_input.split()[1:])
                return 'ok\n\n' + json.dumps(r)
            except Exception as e:
                print(e)
                raise Exception('Error with db')
        elif put_pat.match(n_input):
            try:
                await BaseServer.put(*n_input.split()[1:])
                return 'ok\n\n'
            except Exception as e:
                print(e)
                raise Exception('Error with db')
        else:
            raise Exception('Invlid Command')

    def run_server(self):
        print('Server Run')
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            print('Server stop')

        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())
        self.loop.close()
