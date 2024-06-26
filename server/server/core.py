import asyncio
import base64
import json
import logging
import os
import shutil
import sys
import time

import websockets

from JIM.config import *
from JIM.utils import send_msg
from server.database.database import MainDataBase
from server.decoss.logger import loggers
from server.utilities.corrected_path import PathMaker

log = logging.getLogger('server')


class Server:
    def __init__(self, listen_address, listen_port):
        super().__init__()
        self.clients = []
        self.database = MainDataBase()
        self.path_img = ''
        self.address = listen_address
        self.port = listen_port
        self.running = True
        print(self.address)
        print(self.port)

    @loggers
    async def process_client_message(self, request: dict) -> dict:
        """
        Function for editing response server on user request
        :param request:dict
        :return dict
        """
        if ACTION in request and request[ACTION] == PRESENCE:
            response = {RESPONSE: OK}
            # log.info(request)
        elif ACTION in request and request[ACTION] == MESSAGE and \
                MESSAGE in request and request[MESSAGE] == 'hi':
            response = {RESPONSE: OK, MESSAGE: 'HELLO, SUPERMAN'}
        elif ACTION in request and request[ACTION] == 'disconnected':
            response = {RESPONSE: OK}
            log.info(request)
        elif ACTION in request and request[ACTION] == MESSAGE and \
                request[MESSAGE] == OK:
            response = {RESPONSE: OK, MESSAGE: 'Успешное завершение операции'}
        elif ACTION in request and request[ACTION] == \
                GET_OBJECTS:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_POSTS:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_COMPONENTS:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_COMPONENT_RANKS:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_COMPONENT_TO_POST_LINK:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_BYPASS_STATUS_OBJECT:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_BYPASS_STATUS_POSTS:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_OBJECTS_SYNCHRONIZE:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_POSTS_SYNCHRONIZE:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_COMPONENTS_SYNCHRONIZE:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_COMPONENTS_RANKS_SYNCHRONIZE:
            response = request
        elif ACTION in request and request[ACTION] == \
                'GET_USER_IN_LOCAL_BASE':
            response = request
        elif ACTION in request and request[ACTION] == GET_BYPASS_STATUS_USERS:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_BYPASS_STATUS_USERS_DETAIL:
            response = request
        elif ACTION in request and request[ACTION] == 'CHECK_EMAIL':
            response = request
        elif ACTION in request and request[ACTION] == 'GET_USERS':
            response = request
        elif ACTION in request and request[ACTION] == 'ADD_ACTIVE_USER':
            response = request
        elif ACTION in request and request[ACTION] == 'REMOVE_ACTIVE_USER':
            response = request
        elif ACTION in request and request[ACTION] == 'GET_ACTIVE_USERS':
            response = request
        elif ACTION in request and request[ACTION] == 'GET_USER_SHIFT':
            response = request
        elif ACTION in request and request[ACTION] == 'USER_LOGOUT':
            response = request
        else:
            response = {RESPONSE: ERROR}
            log.error(request)

        return response

    def create_image_file(self, file):
        if not os.path.exists(self.path_img.rsplit(os.sep, 1)[0]):
            os.makedirs(self.path_img.rsplit(os.sep, 1)[0])
        with open(self.path_img, 'wb') as f:
            f.write(base64.b64decode(file[23:].encode('utf-8')))

    async def refresh_elements_no_content(self, action, elements):
        for ws in self.clients:
            await send_msg(ws, await self.process_client_message(
                {ACTION: action, MESSAGE: elements
                 }))

    async def refresh_elements(self, action, added_elements,
                               removed_elements, websocket=None,
                               target_id=None,
                               updated_elements=None):
        send_data = {ACTION: action,
                     EDIT_ELEMENTS: [],
                     UPDATE_ELEMENTS: updated_elements,
                     REMOVE_ELEMENTS: removed_elements,
                     CREATE_ELEMENTS: added_elements,
                     CONTENT: self.get_content_list(added_elements),
                     'CONTENT_UPDATE': self.get_content_list(updated_elements),
                     'TARGET_ID': target_id
                     }
        if websocket:
            await send_msg(websocket,
                           await self.process_client_message(send_data))
        else:
            for ws in self.clients:
                await send_msg(ws,
                               await self.process_client_message(send_data))

    @staticmethod
    def get_content_list(obj):
        content_list = []
        for element in obj:
            with open(element['image'], 'rb') as f:
                content_list.append(
                    base64.b64encode(f.read()).decode('utf-8'))
        return content_list

    @staticmethod
    def get_update_users(all_elements, server_data):
        action_for_base = list({d['id']: d for d in all_elements}.values())
        # action_for_server_base = list({d['id']: d for d in server_data}.values())
        print('action for base', action_for_base)
        print('server ', server_data)
        for el in action_for_base:
            for els in server_data:
                if el['id'] == els['id']:
                    els['img'] = el['img']
                    print(f'{el}: el {els}: els')
                    if el['id'] == els['id'] \
                        and el['surname'] == els['surname'] \
                        and el['name'] == els['name'] \
                        and el['lastname'] == els['lastname'] \
                        and el['position'] == els['position'] \
                        and el['email'] == els['email'] \
                        and el['img'].rsplit('/', 1)[-1].split('.')[0] == \
                        els['image'].rsplit(os.sep, 1)[-1].split('.')[0] \
                        and el['privileg'] == els['privileg'] \
                        and el['start_shift'] == els['start_shift']:
                        server_data.pop(server_data.index(els))
        return server_data

    @staticmethod
    def get_update_component_rank_element(all_elements, server_data):
        """"""
        action_for_base = list({d['id']: d for d in all_elements}.values())
        print(action_for_base)
        print(server_data)
        for el in action_for_base:
            for els in server_data:
                if el['id'] == els['id']:
                    els['img'] = el['img']
                    print(f'{el}: el {els}: els')
                if el['id'] == els['id'] and el['name'] == els['name'] and \
                        float(el['rank']) == float(els['rank']) and \
                        el['img'].rsplit('/', 1)[-1].split('.')[0] == \
                        els['image'].rsplit(os.sep, 1)[-1].split('.')[0]:
                    server_data.pop(server_data.index(els))
        return server_data

    @staticmethod
    def get_list_elements(all_elements, server_or_client_data_list):
        action_for_base = list({d['id']: d for d in all_elements}.values())

        for el in server_or_client_data_list:
            for els in action_for_base:
                if el['id'] == els['id']:
                    action_for_base.pop(action_for_base.index(els))
        return action_for_base

    @staticmethod
    def configurate_path_img(msg) -> str:
        return f"{msg[ACTION].split('_', 1)[1].lower()}{os.sep}{msg[NAME]}" \
            f"{os.sep}{msg[NAME_FILE]}"

    def get_full_path(self, request):
        return os.path.join(os.path.normpath(
            os.path.dirname(os.path.abspath(
                __file__)) + os.sep + os.pardir),
            f'images{os.sep}' + self.configurate_path_img(request)) + '.jpeg'

    def join_path(self, parent, msg, n_split):
        if msg[NAME_FILE]:
            return os.path.join(parent.rsplit(os.sep, n_split)[0],
                                self.configurate_path_img(msg) + '.jpeg')
        return os.path.join(parent.rsplit(os.sep, n_split)[0],
                            msg[NAME] + os.sep + parent.rsplit(os.sep, 1)[-1])

    async def notify_user(self, status) -> None:
        if self.clients:
            await asyncio.wait(
                [user.send(json.dumps({ACTION: MESSAGE, MESSAGE: status})) for
                 user
                 in
                 self.clients])

    async def register(self, websocket) -> None:
        self.clients.append(websocket)
        await self.notify_user('подключился')

    async def unregister(self, websocket) -> None:
        user = self.database.user_critical_logout(str(id(websocket)))
        # print(dir(user_id['user_id']), 'user_id')
        self.clients.remove(websocket)
        for client in self.clients:
            await send_msg(client, await self.process_client_message(
                {ACTION: 'REMOVE_ACTIVE_USER', MESSAGE: getattr(user, 'user_id', None)}))

        await self.notify_user('отключился')

    def start(self) -> None:
        self.init_socket()

    def init_socket(self) -> None:
        # noinspection PyBroadException
        try:
            start_server = websockets.serve(self.hello, self.address,
                                            self.port, max_size=1_000_000_000)
            loop = asyncio.get_event_loop()
            print(start_server)

            loop.run_until_complete(start_server)
            log.info('Сервер запущен')
            loop.run_forever()
        except Exception:
            log.critical('Ошибка создания сервера/Занят '
                         'порт/Занят адрес/Неверные данные')

    async def hello(self, websocket: websockets.WebSocketServerProtocol,
                    path: str) -> None:
        """
        Function for getting and sending message on server
        :param websocket:
        :param path:
        :return: None
        """

        try:
            await self.register(websocket)
            log.info(f'К серверу подключился {websocket.__dict__}')
            print(self.clients)
            async for msg in websocket:
                request = json.loads(msg)
                if ACTION in request and request[ACTION] == CREATE_USER:
                    self.path_img = self.get_full_path(request)
                    is_user_exists = self.database.get_user(request[EMAIL])
                    print(is_user_exists, ' Base')
                    if not is_user_exists:
                        self.database.create_user(request['ID'],
                                                  request[SURNAME],
                                                  request[NAME],
                                                  request[LASTNAME],
                                                  request[POSITION],
                                                  request[EMAIL],
                                                  request[PRIVILEG],
                                                  request[KEY_AUTH],
                                                  request[STATUS],
                                                  self.path_img,
                                                  request[PASSWD_HASH],
                                                  request['START_SHIFT'])
                        self.create_image_file(request['PATH'])

                    await send_msg(websocket,
                                   await self.process_client_message(
                                       {ACTION: GET_USER_IN_LOCAL_BASE,
                                        MESSAGE: is_user_exists if is_user_exists else request}))
                    log.info(request)

                elif ACTION in request and request[ACTION] == ADD_OBJECT:
                    self.path_img = self.get_full_path(request)
                    self.database.create_building(request['ID'],
                                                  request[NAME],
                                                  request[ADDRESS],
                                                  request[DESCRIPTION],
                                                  self.path_img)
                    self.create_image_file(request['PATH'])
                    request['image'] = self.path_img
                    requests = dict(
                        [[k.lower(), v] for k, v in request.items()])
                    request['PATH'] = 0
                    added_element = list()
                    added_element.append(requests)
                    # objects = self.database.get_buildings()
                    # await self.refresh_elements(GET_OBJECTS_SYNCHRONIZE,
                    #                             added_element,
                    #                             removed_elements=[],
                    #                             updated_elements=[])
                    log.info(request)

                elif ACTION in request and request[ACTION] == ADD_POST:
                    parent = self.database.get_building_id(
                        int(request[BUILDING_ID]))
                    self.path_img = self.join_path(parent.image, request,
                                                   n_split=1)
                    self.database.create_post(request['ID'],
                                              request[BUILDING_ID],
                                              request[NAME],
                                              request[DESCRIPTION],
                                              self.path_img,
                                              request[QRCODE],
                                              request[QRCODE_IMG])
                    self.create_image_file(request['PATH'])
                    request['image'] = self.path_img
                    requests = dict(
                        [[k.lower(), v] for k, v in request.items()])
                    request['PATH'] = 0
                    added_element = list()
                    added_element.append(requests)
                    # posts = self.database.get_posts(request[BUILDING_ID])
                    # await self.refresh_elements(GET_POSTS_SYNCHRONIZE,
                    #                             added_element,
                    #                             removed_elements=[],
                    #                             target_id=request[BUILDING_ID],
                    #                             updated_elements=[])

                    log.info(request)

                elif ACTION in request and request[ACTION] == ADD_COMPONENT:
                    self.path_img = self.get_full_path(request)
                    self.database.create_component(request['ID'],
                                                   request[NAME],
                                                   request[DESCRIPTION],
                                                   self.path_img)
                    self.create_image_file(request['PATH'])
                    # components = self.database.get_components()
                    request['image'] = self.path_img
                    requests = dict(
                        [[k.lower(), v] for k, v in request.items()])
                    request['PATH'] = 0
                    added_element = list()
                    added_element.append(requests)

                    # await self.refresh_elements(GET_COMPONENTS_SYNCHRONIZE,
                    #                             added_element,
                    #                             removed_elements=[],
                    #                             updated_elements=[])
                    log.info(request)

                elif ACTION in request and request[ACTION] == \
                        ADD_COMPONENT_RANK:
                    parent = self.database.get_component_id(
                        int(request[COMPONENT_ID]))
                    self.path_img = self.join_path(parent.image, request,
                                                   n_split=1)

                    self.database.create_component_rank(
                        request['ID'],
                        int(request[COMPONENT_ID]),
                        request[NAME],
                        request[RANK],
                        self.path_img)
                    self.create_image_file(request['PATH'])
                    request['image'] = self.path_img
                    requests = dict(
                        [[k.lower(), v] for k, v in request.items()])
                    request['PATH'] = 0
                    added_element = list()
                    added_element.append(requests)

                    # await self.refresh_elements(
                    #     GET_COMPONENTS_RANKS_SYNCHRONIZE,
                    #     added_element,
                    #     removed_elements=[],
                    #     target_id=request[COMPONENT_ID],
                    #     updated_elements=[])
                    log.info(request)

                elif ACTION in request and request[ACTION] == \
                        EDIT_COMPONENT_RANK:
                    name_directory = request[NAME]
                    key_editor = 'no_photo'
                    path_exists = self.database.get_component_rank_for_id(
                        request['ID']).image

                    if request[NAME_FILE]:
                        key_editor = 'photo'
                        self.path_img = self.join_path(path_exists,
                                                       request, n_split=3)
                        request[IMG] = self.path_img
                    else:
                        request[IMG] = self.join_path(path_exists, request,
                                                      n_split=2)

                    self.database.edit_component_rank(request)

                    print(request)
                    PathMaker.change_path(path_exists.rsplit(os.sep, 1)[0],
                                          name_directory, key_editor)
                    self.create_image_file(request['PATH'])
                    request['PATH'] = 0
                    request[IMG] = request[IMAGE]
                    requests = dict(
                        [[k.lower(), v] for k, v in request.items()])
                    print(request)
                    updated_elements = list()
                    updated_elements.append(requests)
                    # await self.refresh_elements(GET_COMPONENTS_RANKS_SYNCHRONIZE,
                    #                             added_elements=[],
                    #                             removed_elements=[],
                    #                             target_id=request[COMPONENT_ID],
                    #                             updated_elements=updated_elements)
                    log.info(request)

                elif ACTION in request and request[ACTION] == \
                        REMOVE_COMPONENT_RANK:
                    path = self.database.remove_component_rank(
                        request['ID'])
                    if path:
                        shutil.rmtree(path.rsplit(os.sep, 1)[0])
                        remove_element = list()
                        remove_element.append({'id': request['ID']})
                        print(remove_element)
                    # posts = self.database.get_posts(request[BUILDING_ID])
                    # await self.refresh_elements(GET_COMPONENTS_RANKS_SYNCHRONIZE,
                    #                             added_elements=[],
                    #                             removed_elements=remove_element,
                    #                             target_id=request[COMPONENT_ID],
                    #                             updated_elements=[])
                    log.info(request)

                elif ACTION in request and request[ACTION] == \
                        CREATE_COMPONENT_TO_POST_LINK:
                    self.database.create_component_to_post_link(
                        request['ID'],
                        request[POST_ID], request[COMPONENT_ID])

                elif ACTION in request and request[ACTION] == \
                        DELETE_COMPONENT_TO_POST_LINK:
                    self.database.remove_component_to_post_link(
                        request[POST_ID], request[COMPONENT_ID])

                elif ACTION in request and request[ACTION] == REMOVE_COMPONENT:
                    path = self.database.remove_component(
                        request[COMPONENT_ID])
                    shutil.rmtree(path.rsplit(os.sep, 1)[0])
                    # components = self.database.get_components()
                    remove_element = list()
                    remove_element.append({'id': request[COMPONENT_ID]})
                    # await self.refresh_elements(GET_COMPONENTS_SYNCHRONIZE,
                    #                             added_elements=[],
                    #                             removed_elements=remove_element,
                    #                             updated_elements=[])

                elif ACTION in request and request[ACTION] == REMOVE_POST:
                    path = self.database.remove_post(request[POST_ID])
                    shutil.rmtree(path.rsplit(os.sep, 1)[0])
                    remove_element = list()
                    remove_element.append({'id': request[POST_ID]})
                    print(remove_element)
                    # posts = self.database.get_posts(request[BUILDING_ID])
                    # await self.refresh_elements(GET_POSTS_SYNCHRONIZE,
                    #                             added_elements=[],
                    #                             removed_elements=remove_element,
                    #                             target_id=request[BUILDING_ID],
                    #                             updated_elements=[])

                elif ACTION in request and request[ACTION] == REMOVE_OBJECT:
                    path = self.database.remove_building(request[BUILDING_ID])
                    print(path, 'REMOVE_OBJECT')
                    remove_element = list()
                    remove_element.append({'id': request[BUILDING_ID]})
                    shutil.rmtree(path.rsplit(os.sep, 1)[0])

                    # await self.refresh_elements(GET_OBJECTS_SYNCHRONIZE,
                    #                             added_elements=[],
                    #                             removed_elements=remove_element,
                    #                             updated_elements=[])

                elif ACTION in request and request[ACTION] == REMOVE_EMPLOEE:
                    self.database.remove_user(request[USER_ID])

                elif ACTION in request and request[ACTION] == CREATE_BYPASS:
                    self.database.create_bypass(request['ID'],
                                                request[USER_ID],
                                                request[POST_ID],
                                                request[START_TIME],
                                                request[WEATHER],
                                                request[TEMPERATURE],
                                                request['ICON'])

                elif ACTION in request and request[ACTION] == \
                        CREATE_BYPASS_RANK:
                    self.database.create_bypass_rank(request['ID'],
                                                     request[BYPASS_ID],
                                                     request[COMPONENT_ID],
                                                     request[START_TIME])

                elif ACTION in request and request[ACTION] == \
                        UPDATE_BYPASS_RANK:
                    print(request)
                    self.database.update_bypass_rank(
                        request[COMPONENT_RANK_ID], request[BYPASS_RANK_ID],
                        request[END_TIME])

                elif ACTION in request and request[ACTION] == UPDATE_BYPASS:
                    self.database.update_bypass(request[AVG_RANK],
                                                request[BYPASS_ID])

                elif ACTION in request and request[ACTION] == FINISHED_BYPASS:
                    self.database.finished_bypass(request[AVG_RANK],
                                                  request[BYPASS_ID],
                                                  request[END_TIME])

                elif ACTION in request and request[ACTION] == \
                        CLEANER_ON_BYPASS:
                    self.database.is_cleaner_on_bypass(
                        request[CLEANER_ON_BYPASS], request[BYPASS_ID])
                    # await send_msg(websocket, \
                    # await process_client_message(request))
                    # fix bug autoincrement
                elif ACTION in request and request[ACTION] == 'GET_USERS':
                    users = self.database.get_users()

                    client_elements = request['LOCAL_DATABASE']
                    print('client', client_elements)
                    client_server = client_elements + users

                    added_elements = self.get_list_elements(client_server,
                                                            client_elements)

                    removed_elements = self.get_list_elements(client_server,
                                                              users)
                    updated_elements = self.get_update_users(client_elements,
                                                             users)
                    for el in added_elements:
                        for els in updated_elements:
                            if el['id'] == els['id']:
                                updated_elements.pop(updated_elements.index(els))
                    print(f'{added_elements} add \n{removed_elements} remove'
                          f' \n{updated_elements} update')

                    print(client_elements)

                    await self.refresh_elements('GET_USERS', added_elements,
                                                removed_elements, websocket,
                                                updated_elements=updated_elements)

                elif ACTION in request and request[ACTION] == GET_OBJECTS:
                    # get elements from server database
                    buildings = self.database.get_buildings()

                    # record elements from client app
                    client_elements = request['LOCAL_DATABASE']

                    # unite elements with server and with client databases
                    client_server = client_elements + buildings

                    # add elements on client which not to server
                    added_elements = self.get_list_elements(client_server,
                                                            client_elements)

                    # remove elements on client app which not to server
                    removed_elements = self.get_list_elements(client_server,
                                                              buildings)

                    print(f'{added_elements} add {removed_elements} remove')
                    await self.refresh_elements(GET_OBJECTS_SYNCHRONIZE,
                                                added_elements,
                                                removed_elements,
                                                websocket,
                                                updated_elements=[])

                elif ACTION in request and request[ACTION] == GET_POSTS:
                    # get elements from server database
                    print(request)
                    posts = self.database.get_posts(request[MESSAGE])

                    # record elements from client app
                    client_elements = request['LOCAL_DATABASE']

                    # unite elements with server and with client databases
                    client_server = client_elements + posts

                    # add elements on client which not to server
                    added_elements = self.get_list_elements(client_server,
                                                            client_elements)

                    # remove elements on client app which not to server
                    removed_elements = self.get_list_elements(client_server,
                                                              posts)

                    print(f'{added_elements} add {removed_elements} remove')
                    await self.refresh_elements(GET_POSTS_SYNCHRONIZE,
                                                added_elements,
                                                removed_elements,
                                                websocket,
                                                request[MESSAGE],
                                                updated_elements=[])

                    print(posts)
                    # await self.refresh_elements(GET_POSTS, posts)

                elif ACTION in request and request[ACTION] == GET_COMPONENTS:
                    # get elements from server database
                    components = self.database.get_components()

                    # record elements from client app
                    client_elements = request['LOCAL_DATABASE']

                    # unite elements with server and with client databases
                    client_server = client_elements + components

                    # add elements on client which not to server
                    added_elements = self.get_list_elements(client_server,
                                                            client_elements)

                    # remove elements on client app which not to server
                    removed_elements = self.get_list_elements(client_server,
                                                              components)

                    print(f'{added_elements} add {removed_elements} remove')
                    await self.refresh_elements(GET_COMPONENTS_SYNCHRONIZE,
                                                added_elements,
                                                removed_elements,
                                                websocket,
                                                updated_elements=[])

                elif ACTION in request and request[ACTION] == \
                        GET_COMPONENT_RANKS:
                    # get elements from server database
                    ranks = self.database.get_component_rank_id(
                        request[MESSAGE])
                    print(ranks)

                    # record elements from client app
                    client_elements = request['LOCAL_DATABASE']

                    # unite elements with server and with client databases
                    client_server = client_elements + ranks

                    # add elements on client which not to server
                    added_elements = self.get_list_elements(client_server,
                                                            client_elements)

                    # remove elements on client app which not to server
                    removed_elements = self.get_list_elements(client_server,
                                                              ranks)
                    updated_elements = self.get_update_component_rank_element(
                        client_elements,
                        ranks
                    )
                    print(request["LOCAL_DATABASE"])
                    for el in added_elements:
                        for els in updated_elements:
                            if el['id'] == els['id'] and el['rank'] == \
                                    els['rank']:
                                updated_elements.pop(
                                    updated_elements.index(els))
                    print(f'{added_elements} add {removed_elements} remove '
                          f'{updated_elements} update')
                    await self.refresh_elements(
                        GET_COMPONENTS_RANKS_SYNCHRONIZE,
                        added_elements,
                        removed_elements,
                        websocket,
                        request[MESSAGE],
                        updated_elements)

                elif ACTION in request and request[ACTION] == \
                        UPDATE_COMPONENT_RANK:
                    self.database.update_component_rank(
                        request[COMPONENT_RANK], request[COMPONENT_LENGTH],
                        request[COUNT])

                elif ACTION in request and request[ACTION] == \
                        GET_COMPONENT_TO_POST_LINK:
                    links = self.database.get_component_to_post_links(
                        request[MESSAGE])
                    print(links)
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       {ACTION: GET_COMPONENT_TO_POST_LINK,
                                        MESSAGE: links}))
                elif ACTION in request and request[ACTION] == \
                        GET_BYPASS_STATUS_OBJECT:
                    print(request[PERIOD], ' - PERIOD')
                    status_object = self.database.get_status_object(
                        request[PERIOD])
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       {ACTION: GET_BYPASS_STATUS_OBJECT,
                                        MESSAGE: status_object}))
                elif ACTION in request and request[ACTION] == \
                        GET_BYPASS_STATUS_POSTS:
                    status_posts = self.database.get_status_posts(
                        request['OBJECT_NAME'], request[PERIOD])
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       {ACTION: GET_BYPASS_STATUS_POSTS,
                                        MESSAGE: status_posts}))
                    print(request[PERIOD], ' - period ',
                          request['OBJECT_NAME'])

                elif ACTION in request and request[ACTION] == \
                        GET_BYPASS_STATUS_USERS:
                    status_users = self.database.get_status_users(
                        request['POST_NAME'], request[PERIOD])
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       {ACTION: GET_BYPASS_STATUS_USERS,
                                        MESSAGE: status_users}
                                   ))
                elif ACTION in request and request[ACTION] == \
                        GET_BYPASS_STATUS_USERS_DETAIL:
                    print(request)
                    status_users_detail = self.database.get_status_users_detail(
                        request[PERIOD],
                        f"'{request['POST_NAME']}'",
                        f"'{request['USER_EMAIL']}'"
                    )
                    print(status_users_detail)
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       {ACTION: GET_BYPASS_STATUS_USERS_DETAIL,
                                        MESSAGE: status_users_detail}
                                   ))
                elif ACTION in request and request[ACTION] == 'CHECK_EMAIL':
                    exists_email = self.database.get_user_email(request[EMAIL])
                    print(exists_email)
                    await send_msg(websocket, await self.process_client_message(
                        {ACTION: 'CHECK_EMAIL',
                         MESSAGE: exists_email}
                    ))
                elif ACTION in request and request[ACTION] == 'ADD_ACTIVE_USER':
                    self.database.user_login(request['ID'], websocket.remote_address[0], 456,
                                             int(time.time() * 1000) + 10800000,
                                             str(id(websocket)))
                    print(f'IP ADDR: {websocket.remote_address[0]} ALL: {websocket.remote_address}')
                    await self.refresh_elements_no_content('ADD_ACTIVE_USER',
                                                           [request['ID']])
                    print(id(websocket.ws_handler))

                elif ACTION in request and request[ACTION] == 'GET_ACTIVE_USERS':
                    active_users = self.database.get_active_users()
                    await self.refresh_elements_no_content('GET_ACTIVE_USERS',
                                                           active_users)
                # for client in self.clients:
                #     await send_msg(client, await self.process_client_message(
                #         {ACTION: MESSAGE, MESSAGE: 'hi'}))
                elif ACTION in request and request[ACTION] == 'UPDATE_EMPLOEE_PRIVILEG':
                    print(request['EMPLOEE']['privileg'])
                    self.database.update_emploee_privileg(request['EMPLOEE']['privileg'],
                                                          request['EMPLOEE']['id'])
                elif ACTION in request and request[ACTION] == 'CREATE_USER_SHIFT':
                    print(request)
                    user = self.database.get_user_shift(request['USER_ID'])
                    if user:
                        if (int(time.time() * 1000) - int(user['create_date'])) > 600000:
                            self.database.create_user_shift(request['USER_ID'], request['START_SHIFT'])
                        else:
                            self.database.update_user_shift(request['USER_ID'], request['START_SHIFT'])
                    else:
                        self.database.create_user_shift(request['USER_ID'],
                                                        request['START_SHIFT'])
                elif ACTION in request and request[ACTION] == 'GET_USER_SHIFT':
                    user_shift = self.database.get_user_shift(request['USER_ID'])
                    print(user_shift)
                    await self.refresh_elements_no_content('GET_USER_SHIFT',
                                                           user_shift)
                elif ACTION in request and request[ACTION] == 'USER_LOGOUT':
                    user_log = self.database.user_logout(request['EMAIL'], str(id(websocket)))

                    if user_log['user_count'] == 1:
                        print(user_log, 'user_log')
                        await self.refresh_elements_no_content('USER_LOGOUT',
                                                               user_log['data'])
        finally:
            await self.unregister(websocket)


if __name__ == '__main__':
    print(os.path.normpath(os.path.dirname(os.path.abspath(__file__)) +
                           os.sep + os.pardir))
    if sys.argv and len(sys.argv):
        if '-a' in sys.argv:
            address = sys.argv[2]
        if '-p' in sys.argv:
            port = int(sys.argv[4])
            print(type(port))

    server = Server('192.168.1.4', 8765)
    server.start()

    print(sys.argv)
