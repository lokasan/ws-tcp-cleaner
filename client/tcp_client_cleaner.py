import asyncio
import logging
import sys
import time
import websockets as ws
from JIM.utils import send_msg, get_msg
from JIM.config import *
from decos.logger import loggers

import json
log = logging.getLogger('client')

HOST = '192.168.1.4'
PORT = 8765


@loggers
async def create_presence(account_name: str = "Guest") -> dict:
    """
    Function create presence for server
    :param account_name: string
    :return: presence: dict
    """
    presence = {
        ACTION: PRESENCE,
        TIME: time.time(),
        USER: {
            ACCOUNT_NAME: account_name
        }
    }
    log.info(presence)
    return presence


@loggers
async def process_ans(message: dict) -> str:
    """
    Function for editing response from server
    :param message: dict
    :return: corrected string response
    """
    if RESPONSE in message:
        if message[RESPONSE] == '200':
            log.info(message)
            return '200: OK'
        elif message[RESPONSE] == 'error':
            log.error(message)
            return '400: ERROR'


async def hello(host: str, port: int) -> None:
    """
    Function connected client with server
    :param host: string
    :param port: int
    :return: None
    """
    uri = f'ws://{host}:{port}'
    address = {'host': host, 'port': port}

    try:
        async with ws.connect(uri) as websocket:
            log.info("Соединение с сервером установлено по адресу %(host)s:%(port)d", address)
            await send_msg(websocket, await create_presence("Boris"))
            response = await process_ans(await get_msg(websocket))
            print(response)
    except ConnectionRefusedError:
        log.critical("Не удалось установить соединение по адресу %(host)s:%(port)d. Неверный хост или порт", address)
if __name__ == '__main__':
    if sys.argv and len(sys.argv) > 1:
        print(sys.argv)
        HOST = sys.argv[1]
        PORT = int(sys.argv[2])

    asyncio.run(hello(HOST, PORT))
