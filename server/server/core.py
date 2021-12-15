import asyncio
import base64
import pathlib
import json
import logging
import os
import shutil
import sys
import time
import ssl
import aiofiles
import re
from send_mail.modules.SMTPClient import SMTPClient
import websockets
import redis
from JIM.config import *
from JIM.utils import send_msg
from server.database.database import MainDataBase
from server.decoss.logger import loggers
from server.utilities.corrected_path import PathMaker

log = logging.getLogger('server')


class Server:
    def __init__(self, listen_address='192.168.1.11', listen_port=8760):
        super().__init__()
        self.clients = []
        self.database = MainDataBase()
        self.red = redis.Redis(host='192.168.1.14')
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
            'GET_ALL_POSTS_FROM_SERVER':
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
                GET_CORPUS_SYNCHRONIZE:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_OBJECTS_SYNCHRONIZE:
            response = request
        elif ACTION in request and request[ACTION] == \
                'GET_OBJECTS_SYNCHRONIZE_ID':
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
                GET_USER_IN_LOCAL_BASE:
            response = request
        elif ACTION in request and request[ACTION] == GET_BYPASS_STATUS_USERS:
            response = request
        elif ACTION in request and request[ACTION] == \
                GET_BYPASS_STATUS_USERS_DETAIL:
            response = request
        elif ACTION in request and request[ACTION] == CHECK_EMAIL:
            response = request
        elif ACTION in request and request[ACTION] == GET_USERS:
            response = request
        elif ACTION in request and request[ACTION] == ADD_ACTIVE_USER:
            response = request
        elif ACTION in request and request[ACTION] == REMOVE_ACTIVE_USER:
            response = request
        elif ACTION in request and request[ACTION] == GET_ACTIVE_USERS:
            response = request
        elif ACTION in request and request[ACTION] == GET_USER_SHIFT:
            response = request
        elif ACTION in request and request[ACTION] == USER_LOGOUT:
            response = request
        elif ACTION in request and request[ACTION] == CHECK_AUTHENTICATION:
            response = request
        elif ACTION in request and request[ACTION] == UPDATE_EMPLOEE_PRIVILEG:
            response = request
        elif ACTION in request and request[ACTION] == GET_BYPASS_STATUS_OBJECT_DETAIL:
            response = request
        elif ACTION in request and request[ACTION] == GET_IMAGE_FOR_BYPASS:
            response = request
        elif ACTION in request and request[ACTION] == GET_BYPASS_RANK_IMAGE_COUNT:
            response = request
        elif ACTION in request and request[ACTION] == GET_SINGLE_USER_STAT:
            response = request
        elif ACTION in request and request[ACTION] == GET_USERS_BASIC_STAT:
            response = request
        elif ACTION in request and request[ACTION] == GET_LIST_USERS_AVERAGE_FOR_POST:
            response = request
        elif ACTION in request and request[ACTION] == GET_BYPASS_STATUS_USERS_DETAIL_FOR_DAY:
            response = request
        elif ACTION in request and request[ACTION] == 'GET_STATUS_USER_WITH_TBR':
            response = request
        elif ACTION in request and request[ACTION] == 'GET_STATUS_USER_WITH_TBR_DETAIL':
            response = request
        elif ACTION in request and request[ACTION] == 'GET_IMAGE_BYPASS_USER_OF_POST_COUNT':
            response = request
        elif ACTION in request and request[ACTION] == 'GET_STATUS_COMPONENT_FOR_BUILDING':
            response = request
        else:
            response = {RESPONSE: ERROR}
            log.error(request)

        return response

    async def create_image_file(self, file):
        if not os.path.exists(self.path_img.rsplit(os.sep, 1)[0]):
            os.makedirs(self.path_img.rsplit(os.sep, 1)[0])
        async with aiofiles.open(self.path_img, 'wb') as f:
            print('GET BASE CODE', file[:24].encode('utf-8'))
            decoding_file = file.split(',', 1)[-1]
            await f.write(base64.b64decode(decoding_file.encode('utf-8')))

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
                     CONTENT_UPDATE: self.get_content_list(updated_elements),
                     TARGET_ID: target_id
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

    @staticmethod
    def get_bypass_path(bypass_id, bypass_rank_id, file_name, ext='.jpeg'):
        return os.path.join(os.path.normpath(
            os.path.dirname(os.path.abspath(
                __file__)) + os.sep + os.pardir),
            f'images{os.sep}' + 'bypass' + os.sep + str(
                bypass_id) + os.sep + 'bypass_rank' + os.sep + str(
                bypass_rank_id) + os.sep + str(file_name) + ext)

    @staticmethod
    def find_trash_symbols_info(text):
        if re.search(r'[",\'\/\\*<>?:|]', text) is not None:
            return None

    def get_full_path(self, request):
        if self.find_trash_symbols_info(request[NAME]) is not None:
            return None
        return os.path.join(os.path.normpath(
            os.path.dirname(os.path.abspath(
                __file__)) + os.sep + os.pardir),
            f'images{os.sep}' + self.configurate_path_img(request)) + request['EXTENSIONS']

    def join_path(self, parent, msg, n_split):
        if msg[NAME_FILE]:
            return os.path.join(parent.rsplit(os.sep, n_split)[0],
                                self.configurate_path_img(msg) + msg['EXTENSIONS'])
        elif NAME in msg and self.find_trash_symbols_info(msg[NAME]) is None:
            return os.path.join(parent.rsplit(os.sep, n_split)[0],
                                msg[NAME] + os.sep + parent.rsplit(os.sep, 1)[-1])
        else:
            return None

    async def notify_user(self, status) -> None:
        if self.clients:
            await asyncio.wait(
                [user.send(json.dumps({ACTION: MESSAGE, MESSAGE: status})) for
                 user
                 in
                 self.clients])

    async def register(self, websocket) -> None:
        self.clients.append(websocket)
        print('ALL CLIENTS: ', self.clients)
        await self.notify_user('подключился')

    async def unregister(self, websocket) -> None:
        user = {}
        try:
            user = self.database.user_critical_logout(str(id(websocket)))
            print(user, 'PRINTED')
            print(user.__dict__, 'PRINTED')
        except Exception as e:
            print(e)
        finally:
            self.clients.remove(websocket)
            if 'user_count' in user and user['user_count'] == 1:
                for client in self.clients:
                    await send_msg(client, await self.process_client_message(
                        {ACTION: REMOVE_ACTIVE_USER, MESSAGE: user['user_id']}))

        await self.notify_user('отключился')

    def start(self) -> None:
        self.init_socket()

    def init_socket(self) -> None:
        # noinspection PyBroadException
        try:
            start_server = websockets.serve(self.hello, self.address,
                                            self.port,
                                            max_size=1_000_000_000)
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
                    print(request, 'user_request')
                    if self.path_img is not None and not is_user_exists:
                        self.database.create_user(request[ID],
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
                                                  request[START_SHIFT])
                        await self.create_image_file(request['PATH'])

                    # await send_msg(websocket,
                    #                await self.process_client_message(
                    #                    {ACTION: GET_USER_IN_LOCAL_BASE,
                    #                     MESSAGE: is_user_exists if is_user_exists else request}))
                    log.info(request)
                elif ACTION in request and request[ACTION] == ADD_CORPUS:
                    print('I am here!! in add_corpus action')
                    self.path_img = self.get_full_path(request)
                    # before how creating i need know about exists a corpus
                    if self.path_img is not None:
                        self.database.create_corpus(request['ID'],
                                                    request[NAME],
                                                    request[ADDRESS],
                                                    request[DESCRIPTION],
                                                    self.path_img,
                                                    request[COORDS])
                        await self.create_image_file(request['PATH'])
                        request['image'] = self.path_img
                        requests = dict(
                            [[k.lower(), v] for k, v in request.items()]
                        )
                        request['PATH'] = 0
                        added_element = list()
                        added_element.append(requests)

                elif ACTION in request and request[ACTION] == ADD_OBJECT:
                    parent = self.database.get_corpus_id(
                        int(request['CORPUS_ID']))
                    self.path_img = self.join_path(parent.image, request,
                                                   n_split=1)
                    if self.path_img is not None:
                        self.database.create_building(request['ID'],
                                                      request['CORPUS_ID'],
                                                      request[NAME],
                                                      request[ADDRESS],
                                                      request[DESCRIPTION],
                                                      self.path_img)
                        await self.create_image_file(request['PATH'])
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
                    if self.path_img is not None:
                        self.database.create_post(request['ID'],
                                                  request[BUILDING_ID],
                                                  request[NAME],
                                                  request[DESCRIPTION],
                                                  self.path_img,
                                                  request[QRCODE],
                                                  request[QRCODE_IMG])
                        await self.create_image_file(request[PATH])
                        request['image'] = self.path_img
                        requests = dict(
                            [[k.lower(), v] for k, v in request.items()])
                        request[PATH] = 0
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
                    if self.path_img is not None:
                        self.database.create_component(request['ID'],
                                                       request[NAME],
                                                       request[DESCRIPTION],
                                                       self.path_img)
                        await self.create_image_file(request[PATH])
                        # components = self.database.get_components()
                        request['image'] = self.path_img
                        requests = dict(
                            [[k.lower(), v] for k, v in request.items()])
                        request[PATH] = 0
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
                    if self.path_img is not None:
                        self.database.create_component_rank(
                            request['ID'],
                            int(request[COMPONENT_ID]),
                            request[NAME],
                            request[RANK],
                            self.path_img)
                        await self.create_image_file(request[PATH])
                        request['image'] = self.path_img
                        requests = dict(
                            [[k.lower(), v] for k, v in request.items()])
                        request[PATH] = 0
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
                    await self.create_image_file(request[PATH])
                    request[PATH] = 0
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
                        request[ID])
                    if path:
                        shutil.rmtree(path.rsplit(os.sep, 1)[0])
                        remove_element = list()
                        remove_element.append({'id': request[ID]})
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
                        request[ID],
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
                    building_id = self.database.get_building_id_of_post_id(
                        request[POST_ID])
                    path = self.database.remove_post(request[POST_ID])
                    shutil.rmtree(path.rsplit(os.sep, 1)[0])
                    remove_element = list()
                    remove_element.append({'id': request[POST_ID]})
                    print(remove_element)
                    # posts = self.database.get_posts(request[BUILDING_ID])

                    # await self.refresh_elements(GET_POSTS_SYNCHRONIZE,
                    #                             added_elements=[],
                    #                             removed_elements=remove_element,
                    #                             target_id=building_id,
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
                elif ACTION in request and request[ACTION] == REMOVE_CORPUS:
                    path = self.database.remove_corpus(request[CORPUS_ID])
                    remove_element = list()
                    remove_element.append({'id': request[CORPUS_ID]})
                    shutil.rmtree(path.rsplit(os.sep, 1)[0])
                elif ACTION in request and request[ACTION] == REMOVE_EMPLOEE:
                    path = self.database.remove_user(request[USER_ID])
                    print(path, 'REMOVE_EMPLOEE')
                    remove_element = list()
                    remove_element.append({'id': request[USER_ID]})
                    # shutil.rmtree(path.rsplit(os.sep, 1)[0])

                elif ACTION in request and request[ACTION] == CREATE_BYPASS:
                    self.database.create_bypass(request[ID],
                                                request[USER_ID],
                                                request[POST_ID],
                                                request[START_TIME],
                                                request[WEATHER],
                                                request[TEMPERATURE],
                                                request[ICON])

                elif ACTION in request and request[ACTION] == \
                        CREATE_BYPASS_RANK:
                    self.database.create_bypass_rank(request[ID],
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

                    # clear redis db memory
                    self.red.flushall()

                elif ACTION in request and request[ACTION] == \
                        CLEANER_ON_BYPASS:
                    print('MY_CLEAN', request)
                    self.database.is_cleaner_on_bypass(
                        request[CLEANER_ON_BYPASS], request[BYPASS_ID])
                    # await send_msg(websocket, \
                    # await process_client_message(request))
                    # fix bug autoincrement
                elif ACTION in request and request[ACTION] == GET_USERS:
                    users = self.database.get_users()

                    client_elements = request[LOCAL_DATABASE]
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

                    await self.refresh_elements(GET_USERS, added_elements,
                                                removed_elements, websocket,
                                                updated_elements=updated_elements)

                elif ACTION in request and request[ACTION] == GET_CORPUS:
                    print('Hell before real hell')
                    corpus = self.database.get_corpus()
                    print('Hell')
                    client_elements = request[LOCAL_DATABASE]
                    client_server = client_elements + corpus
                    added_elements = self.get_list_elements(client_server,
                                                            client_elements)
                    removed_elements = self.get_list_elements(client_server,
                                                              corpus)
                    await self.refresh_elements(GET_CORPUS_SYNCHRONIZE,
                                                added_elements,
                                                removed_elements,
                                                websocket,
                                                updated_elements=[])
                elif ACTION in request and request == 'GET_CORPUS':
                    corpuses = self.database.get_corpus()
                    client_elements = request[LOCAL_DATABASE]
                    client_server = client_elements + corpuses
                    added_elements = self.get_list_elements(client_server,
                                                            client_elements)
                    removed_elements = self.get_list_elements(client_server,
                                                              corpuses)
                    await self.refresh_elements('GET_CORPUSES_SYNCHRONIZE',
                                                added_elements,
                                                removed_elements,
                                                websocket,
                                                updated_elements=[])
                elif ACTION in request and request[ACTION] == 'GET_OBJECT_BY_ID':
                    # get elements from server database
                    buildings = self.database.get_buildings_id(request['TARGET_ID'])

                    # record elements from client app
                    client_elements = request[LOCAL_DATABASE]

                    # unite elements with server and with client databases
                    client_server = client_elements + buildings

                    # add elements on client which not to server
                    added_elements = self.get_list_elements(client_server,
                                                            client_elements)

                    # remove elements on client app which not to server
                    removed_elements = self.get_list_elements(client_server,
                                                              buildings)

                    print(f'{added_elements} add {removed_elements} remove')
                    await self.refresh_elements('GET_OBJECTS_SYNCHRONIZE_ID',
                                                added_elements,
                                                removed_elements,
                                                websocket,
                                                target_id=request['TARGET_ID'],
                                                updated_elements=[])
                elif ACTION in request and request[ACTION] == GET_OBJECTS:
                    # get elements from server database
                    buildings = self.database.get_buildings()

                    # record elements from client app
                    client_elements = request[LOCAL_DATABASE]

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
                    client_elements = request[LOCAL_DATABASE]

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
                elif ACTION in request and request[ACTION] == 'GET_ALL_POSTS_FROM_SERVER':
                    posts_all = self.database.get_all_posts()
                    # record elements from client app
                    client_elements = request[LOCAL_DATABASE]

                    # unite elements with server and with client databases
                    client_server = client_elements + posts_all

                    # add elements on client which not to server
                    added_elements = self.get_list_elements(client_server,
                                                            client_elements)

                    # remove elements on client app which not to server
                    removed_elements = self.get_list_elements(client_server,
                                                              posts_all)

                    print(f'{added_elements} add {removed_elements} remove')

                    await self.refresh_elements('GET_ALL_POSTS_FROM_SERVER',
                                                added_elements,
                                                removed_elements,
                                                websocket,
                                                target_id=None,
                                                updated_elements=[])
                elif ACTION in request and request[ACTION] == GET_COMPONENTS:
                    # get elements from server database
                    components = self.database.get_components()

                    # record elements from client app
                    client_elements = request[LOCAL_DATABASE]

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
                    client_elements = request[LOCAL_DATABASE]

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
                    print(request[LOCAL_DATABASE])
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
                    print(status_object, 'STATUS_OBJ')
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       {ACTION: GET_BYPASS_STATUS_OBJECT,
                                        MESSAGE: status_object}))
                elif ACTION in request and request[ACTION] == \
                    GET_BYPASS_STATUS_OBJECT_DETAIL:
                    status_object_detail = self.database.get_status_object_detail(
                        request[PERIOD],
                        request[OBJECT_NAME]

                    )

                    print(status_object_detail, 'STATUS_OBJ-detail')
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       {ACTION: GET_BYPASS_STATUS_OBJECT_DETAIL,
                                        MESSAGE: status_object_detail}))
                elif ACTION in request and request[ACTION] == \
                        GET_BYPASS_STATUS_POSTS:
                    start = time.time()
                    if not self.red.exists(f'{GET_BYPASS_STATUS_POSTS}_{request[OBJECT_NAME]}_{request[PERIOD]}'):

                        status_posts = self.database.get_status_posts(
                            request[OBJECT_NAME], request[PERIOD])
                        self.red.set(f'{GET_BYPASS_STATUS_POSTS}_{request[OBJECT_NAME]}_{request[PERIOD]}', json.dumps(status_posts))
                    else:
                        status_posts = json.loads(self.red.get(f'{GET_BYPASS_STATUS_POSTS}_{request[OBJECT_NAME]}_{request[PERIOD]}'))
                    print('INFO-FOR-POSTS', time.time() - start)
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       {ACTION: GET_BYPASS_STATUS_POSTS,
                                        MESSAGE: status_posts}))
                    print(request[PERIOD], ' - period ',
                          request[OBJECT_NAME])

                elif ACTION in request and request[ACTION] == \
                        GET_BYPASS_STATUS_USERS:
                    status_users = self.database.get_status_users(
                        request[POST_NAME], request[PERIOD])
                    print(status_users, 'USERS_STATUS')
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       {ACTION: GET_BYPASS_STATUS_USERS,
                                        MESSAGE: status_users}
                                   ))
                elif ACTION in request and request[ACTION] == \
                        GET_BYPASS_STATUS_USERS_DETAIL:
                    print(request)
                    start = time.time()

                    action_status = GET_BYPASS_STATUS_USERS_DETAIL_FOR_DAY if \
                    request[PERIOD] == 'day' or request[
                        PERIOD] == 'today' else GET_BYPASS_STATUS_USERS_DETAIL
                    if not self.red.exists(f'{action_status}_{request[USER_EMAIL]}_{request[POST_NAME]}_{request[PERIOD]}_{request[START_TIME]}'):
                        status_users_detail = self.database.get_status_users_detail(
                            request[PERIOD],
                            f"'{request[POST_NAME]}'",
                            f"'{request[USER_EMAIL]}'",
                            request[START_TIME]
                        )
                        self.red.set(f'{action_status}_{request[USER_EMAIL]}_{request[POST_NAME]}_{request[PERIOD]}_{request[START_TIME]}', json.dumps(status_users_detail))
                    else:
                        status_users_detail = json.loads(self.red.get(f'{action_status}_{request[USER_EMAIL]}_{request[POST_NAME]}_{request[PERIOD]}_{request[START_TIME]}'))
                    print(status_users_detail)
                    print(f'{time.time() - start}')
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       {
                                           ACTION: action_status,
                                           PERIOD: request[PERIOD],
                                           MESSAGE: status_users_detail}
                                   ))
                elif ACTION in request and request[ACTION] == CHECK_EMAIL:
                    exists_email = self.database.get_user_email(request[EMAIL])
                    print(exists_email)
                    await send_msg(websocket, await self.process_client_message(
                        {ACTION: CHECK_EMAIL,
                         MESSAGE: exists_email}
                    ))
                elif ACTION in request and request[ACTION] == ADD_ACTIVE_USER:
                    self.database.user_login(request[ID],
                                             websocket.remote_address[0],
                                             websocket.remote_address[1],
                                             int(
                                                 time.time() * 1000) + 10800000,
                                             str(id(websocket)))
                    print(f'IP ADDR: {websocket.remote_address[0]} ALL: {websocket.remote_address}')
                    await self.refresh_elements_no_content(ADD_ACTIVE_USER,
                                                           [request[ID]])
                    print(id(websocket.ws_handler))

                elif ACTION in request and request[ACTION] == GET_ACTIVE_USERS:
                    active_users = self.database.get_active_users()
                    await self.refresh_elements_no_content(GET_ACTIVE_USERS,
                                                           active_users)
                # for client in self.clients:
                #     await send_msg(client, await self.process_client_message(
                #         {ACTION: MESSAGE, MESSAGE: 'hi'}))
                elif ACTION in request and request[ACTION] == UPDATE_EMPLOEE_PRIVILEG:
                    print(request[EMPLOEE]['privileg'])
                    update_privileg = {
                        'privileg': request[EMPLOEE]['privileg'],
                        'id': request[EMPLOEE]['id'],
                    }
                    self.database.update_emploee_privileg(request[EMPLOEE]['privileg'],
                                                          request[EMPLOEE]['id'])
                    await self.refresh_elements_no_content(UPDATE_EMPLOEE_PRIVILEG,
                                                           update_privileg)
                elif ACTION in request and request[ACTION] == CREATE_USER_SHIFT:
                    print(request)
                    user = self.database.get_user_shift(request[USER_ID])
                    if user:
                        if (int(time.time() * 1000) - int(user['create_date'])) > 600000:
                            self.database.create_user_shift(request[USER_ID], request[START_SHIFT])
                        else:
                            self.database.update_user_shift(request[USER_ID], request[START_SHIFT])
                    else:
                        self.database.create_user_shift(request[USER_ID],
                                                        request[START_SHIFT])
                elif ACTION in request and request[ACTION] == GET_USER_SHIFT:
                    user_shift = self.database.get_user_shift(request[USER_ID])
                    print(user_shift)
                    await self.refresh_elements_no_content(GET_USER_SHIFT,
                                                           user_shift if user_shift else {
                                                               'user_id':
                                                                   request[
                                                                       USER_ID],
                                                               'start_shift': int(
                                                                   time.time() * 1000)})
                elif ACTION in request and request[ACTION] == USER_LOGOUT:
                    user_log = self.database.user_logout(request[EMAIL], str(id(websocket)))

                    if user_log['user_count'] == 1:
                        print(user_log, 'user_log')
                        await self.refresh_elements_no_content(USER_LOGOUT,
                                                               user_log['data'])
                elif ACTION in request and request[ACTION] == CHECK_AUTHENTICATION:
                    user = self.database.get_user_for_authentication(request[EMAIL], request[PASSWORD])
                    print(user, 'AUTHENTICATE')
                    email = getattr(user, 'email', None)
                    ids = getattr(user, 'id', None)

                    await send_msg(websocket,
                                   await self.process_client_message(
                                       {ACTION: CHECK_AUTHENTICATION,
                                        MESSAGE: {'email': email,
                                                  'id': ids}}))
                    # else:
                    #     await self.refresh_elements_no_content(CHECK_AUTHENTICATION,
                    #                                            {
                    #                                                'email': email,
                    #                                                'id': ids,
                    #                                            })
                elif ACTION in request and request[ACTION] == SEND_MESSAGE_TO_MAIL:
                    emails = [request[EMAIL]]
                    my_text = "<html>" \
                              "<head></head>" \
                              "<body>" \
                              "<p><b>{0} {1} {2}</b>, Вас зарегистрировали в приложении NSClean</p>" \
                              "<p>Ваши авторизационные данные:<br>" \
                              "Логин: <b>{3}</b><br>" \
                              "Пароль: <b>{4}</b><br>"\
                              "Время начала обходов: <b>{5}</b>"\
                              "</p>" \
                              "</body>" \
                              "</html>"
                    SMTP = SMTPClient('nsclean@neweducations.online', '290590120nN')
                    SMTP.send_message(
                        'Успешная регистрация в приложении NSClean',
                        my_text.format(request[SURNAME], request[NAME], request[LASTNAME],
                                       request[EMAIL], request[PASSWORD], request[START_SHIFT]),
                        emails)
                elif ACTION in request and request[ACTION] == EDIT_BYPASSRANK_AND_IMAGE:
                    print(len(request[PATH]), 'len of path')
                    try:
                        for el in request[PATH]:
                            self.path_img = self.get_bypass_path(
                                request[BYPASS_ID],
                                request[BYPASS_RANK_ID], el['id'])
                            await self.create_image_file(el['image'])
                            self.database.create_photo_rank_gallery(el['id'],
                                                                    request[
                                                                        BYPASS_RANK_ID],
                                                                    self.path_img)
                        self.database.update_bypass_rank(
                            request[COMPONENT_RANK_ID], request[BYPASS_RANK_ID],
                            request[END_TIME], True)
                    except:
                        pass

                elif ACTION in request and request[ACTION] == \
                        'GET_IMAGE_BYPASS_USER_OF_POST_COUNT':
                    images_count = self.database.\
                        get_photo_rank_gallery_count_user_post(
                            request[PERIOD],
                            int(request[COMPONENT_ID]),
                            request[POST_ID],
                            f"'{request[EMAIL]}'",
                            request[START_TIME],
                            request[END_TIME])
                    send_length = {
                        ACTION: 'GET_IMAGE_BYPASS_USER_OF_POST_COUNT',
                        'LENGTH': images_count
                    }
                    print(json.dumps(send_length, indent=4, ensure_ascii=False))
                    await send_msg(websocket,
                                   await self.process_client_message(send_length))
                elif ACTION in request and request[ACTION] == 'GET_IMAGE_BYPASS_USER_OF_POST':
                    images = self.\
                        database.get_photo_user_of_post(request[PERIOD],
                                                    int(request[COMPONENT_ID]),
                                                    request[POST_ID],
                                                    f"'{request[EMAIL]}'",
                                                    request[LIMIT],
                                                    request[OFFSET],
                                                    request[START_TIME],
                                                    request[END_TIME])
                    photo_dict = {
                        ACTION: GET_IMAGE_FOR_BYPASS,
                        BYPASS_RANK_ID: request[COMPONENT_ID],
                        CONTENT: self.get_content_list(images),
                        'DATA': images
                    }
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       photo_dict))

                elif ACTION in request and request[ACTION] == 'GET_IMAGE_FOR_BYPASS_COUNT':
                    images_count = self.database.get_photo_rank_gallery_count(request[BYPASS_RANK_ID])
                    send_length = {
                        ACTION: GET_BYPASS_RANK_IMAGE_COUNT,
                        'LENGTH': images_count
                    }
                    await send_msg(websocket,
                                   await self.process_client_message(send_length))

                elif ACTION in request and request[ACTION] == GET_IMAGE_FOR_BYPASS:
                    images = self.database.get_photo_rank_gallery(request[BYPASS_RANK_ID],
                                                                  request[LIMIT],
                                                                  request[OFFSET])
                    photo_dict = {
                        ACTION: GET_IMAGE_FOR_BYPASS,
                        BYPASS_RANK_ID: request[BYPASS_RANK_ID],
                        CONTENT: self.get_content_list(images)
                    }
                    print(photo_dict)
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       photo_dict))
                elif ACTION in request and request[ACTION] == GET_SINGLE_USER_STAT:
                    user_stat = self.database.get_response_for_universal_query(request[USER_ID])
                    stat_dict = {
                        ACTION: GET_SINGLE_USER_STAT,
                        MESSAGE: user_stat
                    }
                    with open('testik.txt', 'w', encoding='utf-8') as f:
                        f.write(str(user_stat))
                    await send_msg(websocket,
                                   await self.process_client_message(stat_dict))
                elif ACTION in request and request[ACTION] == GET_USERS_BASIC_STAT:

                    users_basic_stat = self.database.get_status_user_basic(
                        None,
                        request[START_TIME],
                        request[END_TIME])
                    user_basic_dict = {
                        ACTION: GET_USERS_BASIC_STAT,
                        MESSAGE: users_basic_stat
                    }
                    with open('include_stat_user.txt', 'w',
                              encoding='utf-8') as f:
                        f.write(str(user_basic_dict))
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       user_basic_dict))
                elif ACTION in request and request[ACTION] == GET_LIST_USERS_AVERAGE_FOR_POST:
                    start = time.time()
                    if not self.red.exists(f'{GET_LIST_USERS_AVERAGE_FOR_POST}_{request[POST_NAME]}_{request[PERIOD]}'):
                        users_stat_avg = self.database.get_list_users_average_for_post(
                            request[PERIOD],
                            request[START_TIME],
                            request[END_TIME],
                            request[POST_NAME]
                        )
                        self.red.set(f'{GET_LIST_USERS_AVERAGE_FOR_POST}_{request[POST_NAME]}_{request[PERIOD]}', json.dumps(users_stat_avg))
                    else:
                        users_stat_avg = json.loads(self.red.get(f'{GET_LIST_USERS_AVERAGE_FOR_POST}_{request[POST_NAME]}_{request[PERIOD]}'))
                    user_stat_avg_dict = {
                        ACTION: GET_LIST_USERS_AVERAGE_FOR_POST,
                        MESSAGE: users_stat_avg
                    }
                    print(users_stat_avg, ' TEST FUNCTIONALITY')
                    print(time.time() - start, 'time execute query')
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       user_stat_avg_dict))
                elif ACTION in request and request[ACTION] == \
                        'GET_STATUS_USER_WITH_TBR':
                    if not self.red.exists(f'{"GET_STATUS_USER_WITH_TBR"}_{request[PERIOD]}_{request[BUILDING_ID]}'):
                        users_stat = self.database.get_status_user_with_tbr(
                            request[PERIOD],
                            request[BUILDING_ID]
                        )
                        self.red.set(f'{"GET_STATUS_USER_WITH_TBR"}_{request[PERIOD]}_{request[BUILDING_ID]}', json.dumps(users_stat))
                    else:
                        users_stat = json.loads(self.red.get(f'{"GET_STATUS_USER_WITH_TBR"}_{request[PERIOD]}_{request[BUILDING_ID]}'))
                    users_stat_dict = {
                        ACTION: 'GET_STATUS_USER_WITH_TBR',
                        MESSAGE: users_stat
                    }
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       users_stat_dict))
                elif ACTION in request and request[ACTION] == \
                    'GET_STATUS_USER_WITH_TBR_DETAIL':
                    if not self.red.exists(f'{"GET_STATUS_USER_WITH_TBR_DETAIL"}_{request[PERIOD]}_{request[USER_ID]}_{request[BUILDING_ID]}'):
                        user_stat_detail = self.database.\
                            get_status_user_with_tbr_detail(
                                request[PERIOD],
                                request[USER_ID],
                                request[BUILDING_ID],
                                request[START_TIME], request[END_TIME])
                        self.red.set(f'{"GET_STATUS_USER_WITH_TBR_DETAIL"}_{request[PERIOD]}_{request[USER_ID]}_{request[BUILDING_ID]}', json.dumps(user_stat_detail))
                    else:
                        user_stat_detail = json.loads(self.red.get(f'{"GET_STATUS_USER_WITH_TBR_DETAIL"}_{request[PERIOD]}_{request[USER_ID]}_{request[BUILDING_ID]}'))
                    user_stat_detail_dict = {
                        ACTION: 'GET_STATUS_USER_WITH_TBR_DETAIL',
                        MESSAGE: user_stat_detail
                    }

                    await send_msg(websocket,
                                   await self.process_client_message(
                                       user_stat_detail_dict))
                elif ACTION in request and request[ACTION] == \
                        'GET_STATUS_COMPONENT_FOR_BUILDING':
                    components = self.database.\
                        get_status_component_with_building(
                            request[PERIOD],
                            request[BUILDING_ID],
                            request[START_TIME],
                            request[END_TIME]
                    )
                    components_dict = {
                        ACTION: 'GET_STATUS_COMPONENT_FOR_BUILDING',
                        MESSAGE: components
                    }
                    await send_msg(websocket,
                                   await self.process_client_message(
                                       components_dict))
        finally:
            print('FINISHED HIM', websocket)
            await self.unregister(websocket)


if __name__ == '__main__':
    print(os.path.normpath(os.path.dirname(os.path.abspath(__file__)) +
                           os.sep + os.pardir))
    address = '192.168.1.11'
    port = 8760
    if sys.argv and len(sys.argv):
        if '-a' in sys.argv:
            address = sys.argv[2]
        if '-p' in sys.argv:
            port = int(sys.argv[4])
            print(type(port))

    server = Server(address, port)
    server.start()

    print(sys.argv)
