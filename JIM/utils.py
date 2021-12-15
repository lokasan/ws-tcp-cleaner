import json
import chardet


def dict_to_bytes(msg):
    return json.dumps(msg)


def bytes_to_dict(msg) -> dict or bytes:
    if isinstance(msg, bytes):
        return msg
    return json.loads(msg)


async def send_msg(websocket, msg) -> None:
    await websocket.send(dict_to_bytes(msg))


async def get_msg(websocket) -> dict:
    return bytes_to_dict(await websocket.recv())