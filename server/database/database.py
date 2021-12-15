import time
import os
import shutil
import psycopg2
from sqlalchemy import Column, BigInteger, Boolean, Integer, String, Text, ForeignKey, \
    create_engine, Table, MetaData, Date, DateTime, desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import BIGINT
from sqlalchemy.sql import text
from sqlalchemy_views import CreateView
from time import time
import random
import pathlib
from server.database.query_text.queries import *
from server.database.query_text.view import view_create, view_drop, view_create_detail, view_drop_detail
from server.utilities.helper_functions.get_today import get_today, get_start_day
from server.utilities.helper_functions.get_start_end_time import get_start_end_time
STOP_BYPASS = 3600

class MainDataBase:
    Base = declarative_base()

    class User(Base):
        """

        """
        __tablename__ = 'user'
        id = Column(BigInteger, primary_key=True)
        surname = Column(String)
        name = Column(String, nullable=False)
        lastname = Column(String)
        position = Column(String)
        email = Column(String, nullable=False, unique=True)
        privileg = Column(Integer, nullable=False)
        key_auth = Column(Text, nullable=False)
        status = Column(Integer, nullable=False)
        image = Column(Text)
        create_user_date = Column(Text, nullable=False)
        bypass = relationship('Bypass', cascade="all,delete", backref='user')
        step_time = relationship('StepTime', cascade="all,delete",
                                 backref='user')
        active_user = relationship('ActiveUser', cascade="all,delete",
                                   backref='user')
        login_history = relationship('LoginHistory', cascade="all,delete",
                                     backref='user')
        user_shift = relationship('UserShift', cascade="all,delete",
                                     backref='user')
        last_conn = Column(Text)
        passwd_hash = Column(String)
        pubkey = Column(Text)
        push_token = Column(Text)
        start_shift = Column(Text)

        def __init__(self, id, surname, name, lastname, position, email,
                     privileg,
                     key_auth, status, image, passwd_hash, start_shift):
            self.id = id
            self.surname = surname
            self.name = name
            self.lastname = lastname
            self.position = position
            self.email = email
            self.privileg = privileg
            self.key_auth = key_auth
            self.status = status
            self.image = image
            self.create_user_date = str(int(time()))
            self.last_conn = str(int(time()))
            self.passwd_hash = passwd_hash
            self.pubkey = None
            self.push_token = None
            self.start_shift = start_shift

    class ActiveUser(Base):
        """

        """
        __tablename__ = 'active_user'
        id = Column(BigInteger, primary_key=True, autoincrement=True)
        user_id = Column(BigInteger, ForeignKey('user.id'))
        ip = Column(Text)
        port = Column(Integer)
        time_conn = Column(Text)
        id_ws = Column(Text)

        def __init__(self, user_id, ip, port, time_conn, id_ws_handler):
            self.user_id = user_id
            self.ip = ip
            self.port = port
            self.time_conn = time_conn
            self.id_ws = id_ws_handler

    class LoginHistory(Base):
        """

        """
        __tablename__ = 'login_history'
        id = Column(BigInteger, primary_key=True, autoincrement=True)
        user_id = Column(BigInteger, ForeignKey('user.id'))
        ip = Column(Text)
        port = Column(Integer)
        last_conn = Column(Text)

        def __init__(self, user_id, ip, port, last_conn):
            self.user_id = user_id
            self.ip = ip
            self.port = port
            self.last_conn = last_conn

    class StepTime(Base):
        """

        """
        __tablename__ = 'step_time'
        id = Column(Integer, primary_key=True, autoincrement=True)
        user_id = Column(BigInteger, ForeignKey('user.id'))
        count_step = Column(Integer)
        date_time = Column(Text, nullable=False)
        current_time = Column(Text, nullable=False)

        def __init__(self, user_id, count_step, date_time, current_time):
            self.user_id = user_id
            self.count_step = count_step
            self.date_time = date_time
            self.current_time = current_time

    class Bypass(Base):
        """

        """
        __tablename__ = 'bypass'
        id = Column(BigInteger, primary_key=True)
        user_id = Column(BigInteger, ForeignKey('user.id'))
        post_id = Column(BigInteger, ForeignKey('post.id'))
        start_time = Column(Text)
        end_time = Column(Text)
        avg_rank = Column(Text)
        weather = Column(Text, nullable=False)
        temperature = Column(Integer, nullable=False)
        cleaner = Column(Integer)
        finished = Column(Integer, nullable=False)
        icon = Column(Text)
        bypass_rank = relationship('BypassRank', cascade="all,delete",
                                   backref='bypass')

        def __init__(self, id, user_id, post_id, start_time,
                     weather, temperature, icon):
            self.id = id
            self.user_id = user_id
            self.post_id = post_id
            self.start_time = start_time
            self.end_time = None
            self.avg_rank = None
            self.weather = weather
            self.temperature = temperature
            self.cleaner = None
            self.icon = icon
            self.finished = 0

    class BypassRank(Base):
        """

        """
        __tablename__ = 'bypass_rank'
        id = Column(BigInteger, primary_key=True)
        bypass_id = Column(BigInteger, ForeignKey('bypass.id'))
        component_id = Column(BigInteger, ForeignKey('component.id'))
        component_rank_id = Column(BigInteger, ForeignKey('component_rank.id'))
        start_time = Column(Text)
        end_time = Column(Text)
        is_image = Column(Boolean)
        photo_rank_gallery = relationship('PhotoRankGallery',
                                          cascade="all,delete",
                                          backref='bypass_rank')
        
        def __init__(self, id, bypass_id, component_id, start_time):
            self.id = id
            self.bypass_id = bypass_id
            self.component_id = component_id
            self.start_time = start_time
            self.end_time = None
            self.is_image = None

    class PhotoRankGallery(Base):
        """

        """
        __tablename__ = 'photo_rank_gallery'
        id = Column(BigInteger, primary_key=True)
        bypass_rank_id = Column(BigInteger, ForeignKey('bypass_rank.id'))
        image = Column(Text)

        def __init__(self, id, bypass_rank_id, image):
            self.id = id
            self.bypass_rank_id = bypass_rank_id
            self.image = image

    class Corpus(Base):
        """

        """
        __tablename__ = 'corpus'
        id = Column(BigInteger, primary_key=True)
        name = Column(String, nullable=False, unique=True)
        description = Column(String)
        address = Column(String)
        coords = Column(Text)
        image = Column(Text)
        building = relationship('Building', cascade='all,delete', backref='corpus')

        def __init__(self, id, name, description, address, coords, image):
            self.id = id
            self.name = name
            self.description = description
            self.address = address
            self.coords = coords
            self.image = image

    class Building(Base):
        """

        """
        __tablename__ = 'building'
        id = Column(BigInteger, primary_key=True)
        corpus_id = Column(BigInteger, ForeignKey('corpus.id'))
        name = Column(String, nullable=False, unique=True)
        address = Column(String)
        description = Column(String)
        image = Column(Text)
        post = relationship('Post', cascade="all,delete", backref='building')

        def __init__(self, id, corpus_id, name, address, description, image):
            self.id = id
            self.corpus_id = corpus_id
            self.name = name
            self.address = address
            self.description = description
            self.image = image

    class Post(Base):
        """

        """
        __tablename__ = 'post'
        id = Column(BigInteger, primary_key=True)
        building_id = Column(BigInteger, ForeignKey('building.id'))
        name = Column(String, nullable=False, unique=True)
        description = Column(String)
        image = Column(Text)
        qr_code = Column(Text, unique=True)
        qr_code_img = Column(Text, unique=True)
        component_with_post = relationship('ComponentWithPost',
                                           cascade="all,delete",
                                           backref='post')

        def __init__(self, id, building_id, name, description, image, qr_code,
                     qr_code_img):
            self.id = id
            self.building_id = building_id
            self.name = name
            self.description = description
            self.image = image
            self.qr_code = qr_code
            self.qr_code_img = qr_code_img

    class ComponentWithPost(Base):
        __tablename__ = 'component_with_post'
        id = Column(BigInteger, primary_key=True)
        post_id = Column(BigInteger, ForeignKey('post.id'))
        component_id = Column(BigInteger, ForeignKey('component.id'))

        def __int__(self, id, post_id, component_id):
            self.id = id
            self.post_id = post_id
            self.component_id = component_id

    class Component(Base):
        """

        """
        __tablename__ = 'component'
        id = Column(BigInteger, primary_key=True)
        name = Column(String, nullable=False, unique=True)
        description = Column(String)
        image = Column(Text, nullable=False)
        component_with_post = relationship('ComponentWithPost',
                                           cascade="all,delete",
                                           backref='component')
        component_rank = relationship('ComponentRank', cascade="all,delete",
                                      backref='component')

        def __init__(self, id, name, description, image):
            self.id = id
            self.name = name
            self.description = description
            self.image = image

    class ComponentRank(Base):
        """

        """
        __tablename__ = 'component_rank'
        id = Column(BigInteger, primary_key=True)
        component_id = Column(BigInteger, ForeignKey('component.id'))
        name = Column(String, nullable=False)
        rank = Column(Text, nullable=False)
        image = Column(Text, nullable=False)
        bypass_rank = relationship('BypassRank', cascade="all,delete",
                                   backref='component_rank')

        def __init__(self, id, component_id, name, rank, image):
            self.id = id
            self.component_id = component_id
            self.name = name
            self.rank = rank
            self.image = image

    class PushNotification(Base):
        __tablename__ = 'push_notification'
        id = Column(BigInteger, primary_key=True)
        user_id = Column(BigInteger, ForeignKey('user.id'))
        push_token = Column(Text, nullable=False)
        
        def __init__(self, user_id, push_token):
            self.user_id = user_id
            self.push_token = push_token

    class UserShift(Base):
        __tablename__ = 'user_shift'
        id = Column(BigInteger, primary_key=True, autoincrement=True)
        user_id = Column(BigInteger, ForeignKey('user.id'))
        start_shift = Column(Text, nullable=False)
        create_date = Column(Text, nullable=False)

        def __init__(self, user_id, start_shift):
            self.user_id = user_id
            self.start_shift = start_shift
            self.create_date = int(time() * 1000)

    class UserCycleOfBypass(Base):
        __tablename__ = 'user_cycle_of_bypass'
        id = Column(BigInteger, primary_key=True, autoincrement=True)
        user_id = Column(BigInteger, ForeignKey('user.id'), nullable=False)
        building_id = Column(BigInteger, ForeignKey('building.id'), nullable=False)
        end_time = Column(Text, nullable=False)
        create_date_row = Column(Date, nullable=False)

    def __init__(self, path='server_base.db3'):
        self.engine = create_engine(f'postgresql+psycopg2://postgres:TeJ1yei8uhe9Ioqu@192.168.1.11:5480/postgres', pool_recycle=7200)
        self.Base.metadata.create_all(self.engine)
        session_factory = sessionmaker(bind=self.engine)
        Session = scoped_session(session_factory)
        self.session = Session()

        # will added clear list function connected users
        self.session.query(self.ActiveUser).delete()
        self.session.commit()
        self._create_default_user()

    def _create_default_user(self):
        print('Hello i am here in create default user')
        if not self.session.query(self.User).count():
            print('Hello i am here in create default user in if not self.session.query')
            surname = 'Administrator'
            name = 'Admin'
            lastname = 'Admin'
            name_default_avatar = 'default.png'
            default_image_src = os.getcwd() + os.sep + name_default_avatar
            print(f'{default_image_src} : image default src')
            destination_image_path = os.path.normpath(
                os.getcwd() + os.sep + os.pardir + os.sep + 'images' + os.sep + 'user' + os.sep + name + os.sep + name_default_avatar)
            print(f'{destination_image_path} destination image path')
            print(os.getcwd(), ' My path to database file')
            print()
            if os.path.isfile(default_image_src):
                if not os.path.exists(destination_image_path.rsplit(os.sep, 1)[0]):
                    os.makedirs(destination_image_path.rsplit(os.sep, 1)[0])
                shutil.copyfile(default_image_src, destination_image_path)
                user = self.User(1, surname, name, lastname, 'main_account',
                                 'nsclean@neweducations.online', 2, '007', 0, destination_image_path,
                                 'header_admin', None)
                self.session.add(user)
                self.session.commit()

    def check_user(self, email: str) -> bool:
        """
        Method check user on exists in database
        :param email: str
        :return: bool
        """
        if self.session.query(self.User).filter_by(email=email).count():
            return True
        return False

    @staticmethod
    def _to_fixed(num: float, digits: float = 0) -> float:
        try:
            return float(f'{num:.{digits}f}')
        except:
            pass

    def get_hash(self, email: str) -> str:
        """
        get hash of user
        :param email: str
        :return: str
        """
        user = self.session.query(self.User).filter_by(email=email).first()
        return user.passwd_hash

    def get_pubkey(self, email: str) -> str:
        """
        get pubkey of user
        :param email: str
        :return: str
        """
        user = self.session.query(self.User).filter_by(email=email).first()
        return user.pubkey

    def get_active_users(self):
        """
        
        :return: 
        """
        active_users_row = self.session.query(self.ActiveUser).all()
        return [getattr(user, 'user_id', 0) for user in active_users_row]

    def user_login(self, user_id, ip, port, time_conn, id_ws_handler) -> None:
        """
        
        :param user_id: 
        :param ip: 
        :param port: 
        :param time_conn: 
        :param id_ws_handler: 
        :return: 
        """
        user_active_from_id = self.session.query(self.ActiveUser).filter_by(
            id_ws=id_ws_handler).first()
        print(user_active_from_id)
        if user_active_from_id:
            user_active_from_id.user_id = user_id
        else:
            active_user_row = self.ActiveUser(int(user_id), ip, port,
                                              time_conn, id_ws_handler)
            self.session.add(active_user_row)
        self.session.commit()
    
    def user_critical_logout(self, id_ws_handler):
        """
        
        :param id_ws_handler: 
        :return: 
        """
        user = self.session.query(self.ActiveUser).filter_by(
            id_ws=id_ws_handler).first()
        user_count = 1
        user_active_many = self.session.query(self.ActiveUser).filter_by(
            user_id=user.user_id).all()
        if isinstance(user_active_many, list):
            user_count = len(user_active_many)
        print('user', user)

        user_exit = self.session.query(self.ActiveUser).filter_by(
            id_ws=id_ws_handler).first()
        user_id = getattr(user, 'user_id', None)
        self.session.delete(user_exit)
        self.session.commit()

        return {'user_id': user.user_id,
                'user_count': user_count}

    def user_logout(self, email: str, id_ws: str) -> dict:
        """
        Method for logout user of server
        :param email: str
        :param id_ws: str
        :return: None
        """
        user = self.session.query(self.User).filter_by(email=email).first()
        user_count = 1
        user_active_many = self.session.query(self.ActiveUser).filter_by(
            user_id=user.id).all()
        if isinstance(user_active_many, list):
            user_count = len(user_active_many)

        user_row = self.session.query(self.ActiveUser).filter_by(
            user_id=user.id, id_ws=id_ws).first()
        user_id = getattr(user_row, 'user_id', None)
        user_row.user_id = None

        self.session.commit()

        return {
            'data': user_id,
            'user_count': user_count
        }

    def users_list(self) -> list:
        """
        get email and last connection time of user
        :return: list
        """
        query = self.session.query(self.User.email, self.User.last_conn)
        return query.all()

    def active_users_list(self) -> list:
        """
        Method get list active users
        :return: None
        """
        query = self.session.query(self.User.email, self.ActiveUser.ip,
                                   self.ActiveUser.port,
                                   self.ActiveUser.time_conn).join(self.User)
        return query.all()

    def login_history(self, email: str = None) -> list:
        """
        Method getting history connection to server
        :param email: str
        :return: list
        """
        query = self.session.query(self.User.email,
                                   self.LoginHistory.last_conn,
                                   self.LoginHistory.ip,
                                   self.LoginHistory.port).join(self.User)
        if email:
            query = query.filter(self.User.email == email)
        return query.all()

    def count_message(self, sender: str, recipient: str) -> None:
        """
        Method counted message recipient and send
        :param sender: str
        :param recipient: str
        :return: None
        """
        sender = self.session.query(self.User).filter_by(
            email=sender).first().id
        recipient = self.session.query(self.User).filter_by(
            email=recipient).first().id
        # sender_row = self.session.query(self.UsersHistory).filter_by(
        #     email=sender).first()
        # sender_row.send += 1
        #
        # recipient_row = self.session.query(self.UsersHistory).filter_by(
        #     email=recipient).first()
        # recipient_row.accepted += 1
        self.session.commit()

    def create_user(self, id, surname, name, lastname, position, email,
                    privileg,
                    key_auth, status, image, passwd_hash, start_shift) -> None:
        """

        :param surname:
        :param name:
        :param lastname:
        :param position:
        :param email:
        :param privileg:
        :param key_auth:
        :param status:
        :param image:
        :param passwd_hash:
        :return: None
        """
        user_row = self.User(id, surname, name, lastname, position, email,
                             privileg,
                             key_auth, status, image,
                             passwd_hash, start_shift)
        self.session.add(user_row)
        self.session.commit()
        # history_row = self.UsersHistory(user_row.id)
        # self.session.add(history_row)
        # self.session.commit()

    def create_user_shift(self, user_id, start_shift) -> None:
        """

        :param user_id:
        :param start_shift:
        :return: None
        """
        user_sh_raw = self.UserShift(user_id, start_shift)
        user_update_row = self.session.query(self.User).filter_by(
            id=user_id).first()
        user_update_row.start_shift = start_shift
        self.session.add(user_sh_raw)
        self.session.commit()

    def update_user_shift(self, user_id, start_shift) -> None:
        """
        
        :param user_id: 
        :param start_shift: 
        :return: None
        """
        user_shift = self.session.query(self.UserShift)\
            .filter_by(user_id=user_id)\
            .order_by(desc(self.UserShift.id))\
            .first()
        user_shift.start_shift = start_shift
        user_update_row = self.session.query(self.User).filter_by(
            id=user_id).first()
        user_update_row.start_shift = start_shift
        self.session.commit()

    def get_user_shift(self, user_id) -> dict:
        """
        
        :param user_id: 
        :return: 
        """
        user_shift = self.session.query(self.UserShift)\
            .filter_by(user_id=user_id)\
            .order_by(desc(self.UserShift.id)).first()
        print(user_shift)
        return {
            'user_id': user_shift.user_id,
            'start_shift': user_shift.start_shift,
            'create_date': user_shift.create_date
        } if user_shift else user_shift

    def remove_user(self, user_id: str) -> str:
        """

        :param user_id:
        :return str: 
        """
        user = self.session.query(self.User).filter_by(id=user_id).first()
        shutil.rmtree(user.image.rsplit(os.sep, 1)[0])
        self.session.delete(user)
        self.session.commit()
        self._create_default_user()
        return user.image

    def get_user(self, email: str) -> dict:
        """

        :param email:
        :return: list
        """
        user = self.session.query(self.User).filter_by(email=email).first()
        if user:
            return {
                'id': user.id,
                'surname': user.surname,
                'name': user.name,
                'lastname': user.lastname,
                'position': user.position,
                'email': user.email,
                'privileg': user.privileg,
                'key_auth': user.key_auth,
                'status': user.status,
                'img': user.image,
                'create_user_date': user.create_user_date
            }
        return user
    
    def get_user_for_authentication(self, login, password):
        """
        
        :param login: 
        :param password: 
        :return: 
        """
        user = self.session.query(self.User).filter_by(email=login,
                                                       passwd_hash=password).first()
        return user
        
    def get_users(self) -> list:
        """

        :return: None
        """
        users = self.session.query(self.User)
        return [
            {
                'id': element.id,
                'surname': element.surname,
                'name': element.name,
                'lastname': element.lastname,
                'position': element.position,
                'email': element.email,
                'privileg': element.privileg,
                'key_auth': element.key_auth,
                'image': element.image,
                'create_user_date': element.create_user_date,
                'last_conn': element.last_conn,
                'start_shift': element.start_shift,
            }
            for element in users.all()]
    
    def create_corpus(self, id, name, address, description, image, coords) \
            -> None:
        """
        
        :param id: 
        :param name: 
        :param address: 
        :param description: 
        :param image: 
        :param coords: 
        :return None:
        """
        corpus_row = self.Corpus(id, name, description, address, coords, image)
        self.session.add(corpus_row)
        self.session.commit()
        
    def remove_corpus(self, id) -> str:
        """

        :param id:
        :return str:
        """
        corpus_row = self.session.query(self.Corpus).filter_by(id=id).first()
        self.session.delete(corpus_row)
        self.session.commit()
        return corpus_row.image

    def get_corpus(self):
        """
        
        :return list: 
        """
        corpus = self.session.query(self.Corpus)
        return [
            {
                'id': element.id,
                'name': element.name,
                'description': element.description,
                'address': element.address,
                'coords': element.coords,
                'image': element.image
            }
            for element in corpus.all()]

    def get_corpus_id(self, id):
        """
        
        :param id: 
        :return: 
        """
        return self.session.query(self.Corpus).filter_by(id=id).first()

    def create_building(self, id, corpus_id, name, address, description, image) -> None:
        """

        :param name:
        :param address:
        :param description:
        :param image:
        :return: None
        """
        building_row = self.Building(id, corpus_id, name, address, description, image)
        print(building_row)
        self.session.add(building_row)
        self.session.commit()

    def remove_building(self, id) -> str:
        """

        :param id:
        :return: None
        """
        building = self.session.query(self.Building).filter_by(id=id).first()
        self.session.delete(building)
        self.session.commit()
        return building.image

    def get_buildings_id(self, corpus_id) -> list:
        """

        :param corpus_id:
        :return:
        """
        buildings = self.session.query(self.Building).filter_by(id=corpus_id)
        return [
            {
                'id': element.id,
                'corpus_id': element.corpus_id,
                'name': element.name,
                'address': element.address,
                'description': element.description,
                'image': element.image
            }
            for element in buildings.all()]

    def get_buildings(self) -> list:
        """

        :return: None
        """
        buildings = self.session.query(self.Building)
        return [
            {
                'id': element.id,
                'corpus_id': element.corpus_id,
                'name': element.name,
                'address': element.address,
                'description': element.description,
                'image': element.image
            }
            for element in buildings.all()]

    def get_building_id(self, id):
        """
        
        :param id: 
        :return: 
        """
        return self.session.query(self.Building).filter_by(id=id).first()
        
    def create_post(self, id, building_id, name, description, image, qr_code,
                    qr_code_img) -> None:
        """

        :param building_id:
        :param name:
        :param description:
        :param image:
        :param qr_code:
        :param qr_code_img:
        :return: None
        """
        post_row = self.Post(id, building_id, name, description, image,
                             qr_code,
                             qr_code_img)
        self.session.add(post_row)
        self.session.commit()

    def remove_post(self, id) -> str:
        """

        :param id:
        :return: None
        """
        post = self.session.query(self.Post).filter_by(id=id).first()
        self.session.delete(post)
        self.session.commit()
        return post.image
    
    def get_building_id_of_post_id(self, id):
        """
        
        :param id: 
        :return: 
        """
        post = self.session.query(self.Post).filter_by(id=id).first()
        return post.building_id
    
    def get_posts(self, building_id) -> list:
        """

        :param building_id:
        :return: list
        """
        posts = self.session.query(self.Post).filter_by(
            building_id=building_id)
        return [
            {
                'building_id': el.building_id,
                'id': el.id,
                'name': el.name,
                'description': el.description,
                'image': el.image,
                'qrcode': el.qr_code,
                'qrcode_img': el.qr_code_img
            }
            for el in posts.all()]
    
    def get_all_posts(self) -> list:
        posts_all = self.session.query(self.Post)
        return [
            {
                'building_id': el.building_id,
                'id': el.id,
                'name': el.name,
                'description': el.description,
                'image': el.image,
                'qrcode': el.qr_code,
                'qrcode_img': el.qr_code_img
            }
            for el in posts_all.all()]
    
    def get_user_email(self, email: str) -> int:
        """
        
        :param email: 
        :return: 
        """
        user = self.session.query(self.User).filter_by(email=email).first()
        return 1 if isinstance(user, self.User) else 0
    
    def get_post(self, id) -> list:
        """

        :param id:
        :return: list
        """
        post = self.session.query(self.Post).filter_by(id=id).first()
        return post

    def create_component(self, id, name, description, image) -> None:
        """

        :param name:
        :param description:
        :param image:
        :return: None
        """
        component_row = self.Component(id, name, description, image)
        self.session.add(component_row)
        self.session.commit()
        return

    def remove_component(self, id) -> str:
        """

        :param id:
        :return: None
        """
        component = self.session.query(self.Component).filter_by(id=id).first()
        self.session.delete(component)
        self.session.commit()
        return component.image

    def get_components(self) -> list:
        """

        :return: list
        """
        components = self.session.query(self.Component)
        return [
            {
                'id': el.id,
                'name': el.name,
                'description': el.description,
                'image': el.image
            }
            for el in components.all()]

    def get_component_id(self, id):
        """

        :param id:
        :return: list
        """
        component = self.session.query(self.Component).filter_by(id=id)
        return component.first()

    def create_component_rank(self, id, component_id, name, rank,
                              image) -> None:
        """

        :param component_id:
        :param name:
        :param rank:
        :param image:
        :return: None
        """
        component_rank = self.ComponentRank(id, component_id, name, rank,
                                            image)
        self.session.add(component_rank)
        self.session.commit()

    def remove_component_rank(self, id) -> str:
        """

        :param id:
        :return: None
        """
        component_rank = self.session.query(self.ComponentRank).filter_by(
            id=id).first()
        if component_rank:
            self.session.delete(component_rank)
            self.session.commit()
            return component_rank.image

    def get_components_ranks(self) -> list:
        """

        :return: list
        """
        components_ranks = self.session.query(self.ComponentRank)
        return components_ranks.all()

    def get_component_rank_for_id(self, id):
        """
        
        :param id: 
        :return:
        """
        path_component_rank = self.session.query(self.ComponentRank).filter_by(
            id=id)
        return path_component_rank.first()

    def get_component_rank_id(self, component_id) -> list:
        """

        :param component_id:
        :return: list
        """
        component_rank = self.session.query(self.ComponentRank).filter_by(
            component_id=component_id)
        return [
            {
                'id': el.id,
                'component_id': el.component_id,
                'name': el.name,
                'rank': el.rank,
                'image': el.image
            }
            for el in component_rank.all()]

    def update_component_rank(self, component_ranks, component_length, count) \
            -> None:
        """
        
        :param component_ranks:
        :param component_length: 
        :param count: 
        :return: None
        """

        for el in component_ranks:
            if el['rank'] != 5:
                empty_component_ranks = float(f"%.2f" % (5 / component_length))
                c_r = self.session.query(self.ComponentRank).filter_by(
                    id=el['id']).first()
                c_r.rank = 5 if empty_component_ranks * count > 5 \
                    else empty_component_ranks * count
                count += 1
                self.session.commit()

    def edit_component_rank(self, component_rank: dict) -> None:
        """

        :param component_rank: str
        :return: None
        """
        c_r = self.session.query(self.ComponentRank).filter_by(
            id=int(component_rank['ID'])).first()
        c_r.name = component_rank['NAME']
        c_r.image = component_rank['IMG']
        self.session.commit()

    def create_component_to_post_link(self, id, post_id, component_id) -> None:
        """

        :param post_id:
        :param component_id:
        :return: None
        """
        exists_row = self.session.query(self.ComponentWithPost).filter_by(
            id=id,
            post_id=post_id,
            component_id=component_id).first()
        if exists_row:
            print(
                f'link post_id:{post_id} with component_id:'
                f'{component_id} don`t created')
        else:
            c_p_l_row = self.ComponentWithPost(post_id=post_id,
                                               component_id=component_id)
            self.session.add(c_p_l_row)
            self.session.commit()

    def remove_component_to_post_link(self, post_id, component_id) -> None:
        """

        :param post_id:
        :param component_id:
        :return: None
        """
        c_p_l_row = self.session.query(self.ComponentWithPost).filter_by(
            post_id=post_id, component_id=component_id).first()
        self.session.delete(c_p_l_row)
        self.session.commit()

    def get_component_to_post_links(self, post_id) -> list:
        """
        
        :param post_id: 
        :return: 
        """
        components = self.session.query(self.Component).join(
            self.ComponentWithPost,
            self.ComponentWithPost.component_id == self.Component.id,
            isouter=True).filter_by(post_id=post_id)
        return [
            {
                'id': el.id,
                'name': el.name,
                'description': el.description,
                'image': el.image
            }
            for el in components.all()]

    def create_bypass(self, id, user_id, post_id, start_time,
                      weather, temperature, icon) -> None:
        """

        :param user_id:
        :param post_id:
        :param start_time:
        :param weather:
        :param temperature:
        :param icon:
        :return: None
        """
        bypass_row = self.Bypass(id, user_id, post_id, start_time,
                                 weather, temperature, icon)
        self.session.add(bypass_row)
        self.session.commit()

    def is_cleaner_on_bypass(self, cleaner, id) -> None:
        """
        
        :param cleaner: 
        :param id: 
        :return: None
        """
        bypass = self.session.query(self.Bypass).filter_by(id=id).first()
        bypass.cleaner = cleaner
        print(bypass.cleaner, 'bypass_is database')
        self.session.commit()

    def update_bypass(self, avg_rank, id) -> None:
        """

        :param avg_rank:
        :param id:
        :return: None
        """
        update_bypass = self.session.query(self.Bypass).filter_by(
            id=id).first()
        update_bypass.avg_rank = avg_rank
        self.session.commit()

    def finished_bypass(self, avg_rank, id, end_time) -> None:
        """

        :param avg_rank:
        :param id:
        :param end_time:
        :return: None
        """
        finished_bypass = self.session.query(self.Bypass).filter_by(
            id=id).first()
        finished_bypass.end_time = end_time
        finished_bypass.avg_rank = avg_rank
        finished_bypass.finished = 1
        self.session.commit()

    def load_bypass(self, user_id, post_id) -> list:
        """

        :param user_id:
        :param post_id:
        :return: list
        """
        bypass = self.session.query(self.Bypass).filter_by(user_id=user_id,
                                                           post_id=post_id,
                                                           finished=0).first()
        if (int(time.time()) - int(bypass.start_time)) > STOP_BYPASS:
            bypass.finished = -1
            self.session.commit()
            return []
        return bypass.first()

    def remove_bypass(self, id) -> None:
        """
        
        :param id: 
        :return: None
        """
        bypass_row = self.session.query(self.Bypass).filter_by(id=id)
        self.session.delete(bypass_row)
        self.session.commit()

    def create_bypass_rank(self, id, bypass_id, component_id,
                           start_time) -> None:
        """

        :param bypass_id:
        :param component_id:
        :param start_time:
        :return: None
        """
        bypass_rank = self.BypassRank(id, bypass_id, component_id, start_time)
        self.session.add(bypass_rank)
        self.session.commit()

    def load_finished_components_for_bypass(self, bypass_id) -> list:
        """

        :param bypass_id:
        :return: list
        """
        finished_components = self.session.query(self.BypassRank).filter_by(
            bypass_id=bypass_id, end_time=not None)
        return finished_components.all()

    def load_started_bypass_rank(self, bypass_id) -> list:
        """

        :param bypass_id:
        :return: list
        """
        started_components = self.session.query(self.BypassRank).filter_by(
            bypass_id=bypass_id, end_time=None)
        return started_components.all()

    def update_emploee_privileg(self, privileg, id) -> None:
        """
        
        :param privileg: 
        :param id: 
        :return: None
        """
        user = self.session.query(self.User).filter_by(id=id).first()
        user.privileg = privileg
        self.session.commit()

    def update_bypass_rank(self, component_rank_id, id, end_time, is_image=False) -> None:
        """

        :param component_rank_id:
        :param id:
        :return: None
        """
        bypass_rank = self.session.query(self.BypassRank).filter_by(
            id=id).first()
        bypass_rank.component_rank_id = component_rank_id
        bypass_rank.end_time = end_time
        bypass_rank.is_image = is_image
        self.session.commit()

    def get_photo_rank_gallery_count(self, bypass_rank_id):
        """
        
        :param bypass_rank_id: 
        :return: int
        """
        return self.session.query(self.PhotoRankGallery).filter_by(
            bypass_rank_id=bypass_rank_id).count()

    def get_photo_rank_gallery_count_user_post(self, period,
                                               component_id,
                                               post_id,
                                               email,
                                               start_time=None,
                                               end_time=None) -> int:
        """

        :param start_time:
        :param end_time:
        :param component_id:
        :param post_id:
        :param email:
        :return: list
        """
        start_time_str, end_time_str = get_start_end_time(period, start_time,
                                                          end_time)

        return len(self.session.execute(QUERY_GET_PHOTO_FOR_USER_OF_POST.format(
            component_id,
            post_id,
            email,
            start_time_str,
            end_time_str,
            'null',
            'null'
        )).all())

    def get_photo_user_of_post(self, period, component_id, post_id, email,
                               limit,
                               offset,
                               start_time=None,
                               end_time=None):
        """

        :param period:
        :param component_id:
        :param post_id: 
        :param email: str
        :param limit:
        :param offset:
        :param start_time:
        :return: list
        """
        start_time_str, end_time_str = get_start_end_time(period, start_time,
                                                          end_time)

        return self.get_photo_rank_gallery_user_post(start_time_str,
                                                     end_time_str,
                                                     component_id,
                                                     post_id,
                                                     email,
                                                     limit,
                                                     offset)

    def get_photo_rank_gallery_user_post(self,
                                         start_time,
                                         end_time,
                                         component_id,
                                         post_id,
                                         email,
                                         limit,
                                         offset) -> list:
        """

        :param start_time:
        :param end_time:
        :param component_id:
        :param post_id:
        :param email:
        :param limit:
        :param offset:
        :return:
        """
        query = self.session.execute(QUERY_GET_PHOTO_FOR_USER_OF_POST.format(
            component_id,
            post_id,
            email,
            start_time,
            end_time,
            limit,
            offset
        ))
        return [
            {
                'component_id': el.component_id,
                'image': el.image,
                'email': el.email,
                'surname': el.surname,
                'name': el.name,
                'lastname': el.lastname,
                'bypass_rank_id': el.bypass_rank_id,
                'time_make_photo': el.time_ms_make_photo,
                'temperature': el.temperature,
                'weather': el.weather,
                'icon': el.icon

            } for el in query
        ]

    def get_photo_rank_gallery(self, bypass_rank_id, limit, offset) -> list:
        """
        
        :param bypass_rank_id: 
        :param limit: 
        :param offset: 
        :return: 
        """
        images_list = self.session.query(self.PhotoRankGallery).filter_by(
            bypass_rank_id=bypass_rank_id).limit(limit).offset(offset)
        return [{
            'bypass_rank_id': bypass_rank_id,
            'image': el.image
        } for el in images_list] 

    def create_photo_rank_gallery(self, id, bypass_rank_id, image) -> None:
        """

        :param bypass_rank_id:
        :param image:
        :return: None
        """
        photo_rank_gallery = self.PhotoRankGallery(id, bypass_rank_id, image)
        self.session.add(photo_rank_gallery)
        self.session.commit()

    def delete_photo_rank_gallery(self, id) -> None:
        """

        :param id:
        :return: None
        """
        photo_rank_gallery = self.session.query(
            self.PhotoRankGallery).filter_by(id=id)
        self.session.delete(photo_rank_gallery)
        self.session.commit()

    def get_status_bypass(self, post_id, finished) -> list:
        """

        :param post_id:
        :param finished:
        :return: None
        """
        status_bypass = self.session.query(self.Bypass).filter_by(
            post_id=post_id, finished=finished)
        return status_bypass.all()

    def create_remove_view_detail(self, create_view, query):
        """
        
        :param create_view: 
        :param query: 
        :return: 
        """
        self.engine.execute(view_create_detail(create_view))
        qrt = self.engine.execute(query).all()
        self.engine.execute(view_drop_detail())
        return qrt
    
    def get_response_for_universal_query(self, user_id):
        """
        
        :param query: 
        :return: 
        """
        user_stat_row = self.engine.execute(
            QUERY_STAT_SINGLE_PERSON.format(user_id, get_today())).all()
        return [
            {
                'id': el[0],
                'avg_rank': float(el[1]) if el[1] else 0,
                'count_bypass': int(el[2]),
                'cycle_bypass': int(el[3])
            }
        for el in user_stat_row]
    
    def create_and_remove_view_and_get_list(self, is_time, create_view, query):
        """

        :param is_time:
        :param create_view:
        :param query:
        :return:
        """
        self.engine.execute(view_create(is_time, create_view))
        qrt = self.engine.execute(query).all()
        self.engine.execute(view_drop())
        return qrt

    def get_status_posts_detail(self, post_name, period):
        """
        
        :param post_name: 
        :param period: 
        :return: 
        """
        if period == 'today':
            return self.get_list_posts_detail(
                get_today(),
                round(get_today() + TAIL_TODAY),
                post_name,
                QUERY_POSTS_DETAIL_LIST
            )

        if period == 'week':
            return self.get_list_posts_detail(
                round(get_today() - WEEK_MILLISECONDS),
                round(get_today() + TAIL_TODAY),
                post_name,
                QUERY_POSTS_DETAIL_LIST)
        if period == 'month':
            self.get_list_posts_detail(
                round(get_today() - MONTH_MILLISECONDS),
                round(time() * 1000),
                post_name,
                QUERY_POSTS_DETAIL_LIST
            )
            return self.get_list_posts_detail(
                round(get_today() - MONTH_MILLISECONDS),
                round(get_today() + TAIL_TODAY),
                post_name,
                QUERY_POSTS_DETAIL_LIST
            )
        if period == 'year':
            return self.get_list_posts_detail(
                round(get_today() - YEAR_MILLISECONDS),
                round(get_today() + TAIL_TODAY),
                post_name,
                QUERY_GET_USERS_DETAIL
            )

    def get_list_posts_detail(self, start_time, end_time, post_name,  query):
        """
        
        :param start_time: 
        :param end_time: 
        :param post_name: 
        :param query: 
        :return: 
        """
        qrt = self.create_remove_view_detail(POSTS_DETAIL_LIST_VIEW.
                                             format(post_name, start_time,
                                                    end_time),
                                             query)
        # print(f'{post_name} post_name')
        id_temp = 0
        temp_list = []
        my_dicts = []
        for idx in range(len(qrt)):
            if id_temp == qrt[idx][0]:
                # print('id_temp equal bypass_id')
                my_dicts[str(qrt[idx][16])] = qrt[idx][14]
                my_dicts[str(qrt[idx][16]) + '_rank'] = qrt[idx][17]
                my_dicts[str(qrt[idx][16]) + '_description'] = qrt[idx][15]
                my_dicts[str(qrt[idx][16]) + '_name_c_r'] = qrt[idx][18]
                # print(my_dicts, 'my_dicts')
            if not id_temp:
                # print('no id_temp')
                my_dicts = {
                    'bypass_id': qrt[idx][0],
                    'user_id': qrt[idx][4],
                    'start_time': qrt[idx][19],
                    'end_time': qrt[idx][20],
                    'weather': qrt[idx][10],
                    'temperature': qrt[idx][11],
                    'cleaner': qrt[idx][12],
                    str(qrt[idx][16]): qrt[idx][14],
                    str(qrt[idx][16]) + '_rank': qrt[idx][17],
                    str(qrt[idx][16]) + '_description': qrt[idx][15],
                    str(qrt[idx][16]) + '_name_c_r': qrt[idx][18],
                    'post_name': qrt[idx][3],
                    'icon': qrt[idx][13],
                    'title': f'{qrt[idx][5]} {qrt[idx][6]} {qrt[idx][7]}',
                    'email': qrt[idx][8]
                }
                id_temp = qrt[idx][0]
            if (id_temp != qrt[idx][0] and id_temp) or (len(qrt) - 1 == idx):
                # print('id_temp not equal bypass_id and not id_temp')
                temp_list.append(my_dicts)
                my_dicts = {
                    'bypass_id': qrt[idx][0],
                    'user_id': qrt[idx][4],
                    'start_time': qrt[idx][19],
                    'end_time': qrt[idx][20],
                    'weather': qrt[idx][10],
                    'temperature': qrt[idx][11],
                    'cleaner': qrt[idx][12],
                    str(qrt[idx][16]): qrt[idx][14],
                    str(qrt[idx][16]) + '_rank': qrt[idx][17],
                    str(qrt[idx][16]) + '_description': qrt[idx][15],
                    str(qrt[idx][16]) + '_name_c_r': qrt[idx][18],
                    'post_name': qrt[idx][3],
                    'icon': qrt[idx][13],
                    'title': f'{qrt[idx][5]} {qrt[idx][6]} {qrt[idx][7]}',
                    'email': qrt[idx][8]
                }
                # print(my_dicts)
                if idx == len(qrt) - 1 and qrt[idx][0] != qrt[idx - 1][0]:
                    temp_list.append(my_dicts)
            id_temp = qrt[idx][0]

        return temp_list

    def get_status_users(self, post_name, period) -> list:
        """

        :param post_name:
        :param period:
        :return: list
        """
        if period == 'today':
            return self.get_list_users(TODAY_MILLISECONDS, post_name)
        if period == 'week':
            return self.get_list_users(WEEK_MILLISECONDS, post_name)
        if period == 'month':
            return self.get_list_users(MONTH_MILLISECONDS, post_name)
        if period == 'year':
            return self.get_list_users(YEAR_MILLISECONDS, post_name)

    def get_status_users_detail(self, period, post_name, user_email, start_time=None):
        """
        
        :param period: 
        :param post_name: 
        :param user_email: 
        :return: 
        """
        if period == 'today':
            return self.get_list_users_detail(
                round(get_today() - TODAY_MILLISECONDS),
                round(get_today() + TAIL_TODAY),
                post_name,
                user_email,
                QUERY_GET_USERS_DETAIL
            )

        if period == 'week':
            return self.get_list_users_detail_week_month(
                round(get_today() - WEEK_MILLISECONDS),
                round(get_today() + TAIL_TODAY),
                post_name,
                user_email,
                USERS_LIST_QUERY_AVG_MONTH_WEEK)
        if period == 'month_range':
            return self.get_list_users_detail_week_month(
                round(start_time),
                round(start_time + MONTH_MILLISECONDS),
                post_name,
                user_email,
                USERS_LIST_QUERY_AVG_MONTH_WEEK
            )
        if period == 'month':
            return self.get_list_users_detail_week_month(
                round(get_today() - MONTH_MILLISECONDS),
                round(get_today() + TAIL_TODAY),
                post_name,
                user_email,
                USERS_LIST_QUERY_AVG_MONTH_WEEK
            )
            # return self.get_list_users_detail(
            #     round(get_today() - MONTH_MILLISECONDS),
            #     round(time() * 1000),
            #     post_name,
            #     user_email,
            #     QUERY_GET_USERS_DETAIL
            # )
        if period == 'year':
            return self.get_list_users_detail_week_month(
                round(get_today() - YEAR_MILLISECONDS),
                round(get_today() + TAIL_TODAY),
                post_name,
                user_email,
                USERS_LIST_QUERY_AVG_YEAR
            )
        if period == 'day':
            return self.get_list_users_detail(
                round(start_time),
                round(start_time + TAIL_TODAY),
                post_name,
                user_email,
                QUERY_GET_USERS_DETAIL
            )

    def get_list_users_average_for_post(self, period, start_time, end_time, post_name):
        start_time_str = 0
        end_time_str = 0
        if period == 'today':
            start_time_str = get_today()
            end_time_str = get_today() + 86400000
        if period == 'week':
            start_time_str = get_today() - WEEK_MILLISECONDS
            end_time_str = get_today() + 86400000
        if period == 'month':
            start_time_str = get_today() - MONTH_MILLISECONDS
            end_time_str = get_today() + 86400000
        if period == 'year':
            start_time_str = get_today() - YEAR_MILLISECONDS
            end_time_str = get_today() + 86400000
        if period == 'range':
            start_time_str = start_time
            end_time_str = start_time + 86400000

        query = USERS_LIST_QUERY_AVG.format(start_time_str, end_time_str, f"'{post_name}'")
        # create_view = USERS_LIST_VIEW % (start_time_str, end_time_str)
        # qrt = self.create_remove_view_detail(create_view, query)
        qrt = self.engine.execute(query).all()
        print(str(qrt), 'This are users of detail info')
        email_tmp = 0
        temp_list = []
        my_dicts = []

        for idx in range(len(qrt)):
            if email_tmp == qrt[idx][0]:
                print('id_temp equal bypass_id')
                my_dicts[str(qrt[idx][9])] = qrt[idx][2]
                my_dicts[str(qrt[idx][9]) + 'count_bad_rank'] = qrt[idx][11]
                my_dicts[str(qrt[idx][9]) + '_rank'] = 0 if qrt[idx][3] is None else float(qrt[idx][3])
                print(my_dicts, 'my_dicts')
            if not email_tmp:
                print('no email_temp')
                my_dicts = {
                    'id': str(time()),
                    'email': qrt[idx][0],
                    'post_name': qrt[idx][1],
                    str(qrt[idx][9]): qrt[idx][2],
                    str(qrt[idx][9]) + '_rank': 0 if qrt[idx][3] is None else float(qrt[idx][3]),
                    'avg_rank': float(qrt[idx][4]),
                    'time_bypass': int(qrt[idx][5]),
                    'count_bypass': qrt[idx][6],
                    'cleaner': None if qrt[idx][7] is None else qrt[idx][7],
                    'time_bbb': None if qrt[idx][8] is None else int(qrt[idx][8]),
                    'post_id': qrt[idx][10],
                    str(qrt[idx][9]) + 'count_bad_rank': qrt[idx][11]
                }
                email_tmp = qrt[idx][0]
            if email_tmp != qrt[idx][0] and email_tmp or (len(qrt) - 1 == idx):
                print('id_temp not equal bypass_id and not id_temp')
                temp_list.append(my_dicts)
                my_dicts = {
                    'id': str(time()),
                    'email': qrt[idx][0],
                    'post_name': qrt[idx][1],
                    str(qrt[idx][9]): qrt[idx][2],
                    str(qrt[idx][9]) + '_rank': None if qrt[idx][3] is None else float(qrt[idx][3]),
                    'avg_rank': float(qrt[idx][4]),
                    'time_bypass': int(qrt[idx][5]),
                    'count_bypass': qrt[idx][6],
                    'cleaner': None if qrt[idx][7] is None else qrt[idx][7],
                    'time_bbb': None if qrt[idx][8] is None else int(qrt[idx][8]),
                    'post_id': qrt[idx][10],
                    str(qrt[idx][9]) + 'count_bad_rank': qrt[idx][11]
                }
                if idx == len(qrt) - 1 and qrt[idx][0] != qrt[idx - 1][0]:
                    temp_list.append(my_dicts)
                print(my_dicts)
            email_tmp = qrt[idx][0]
        return temp_list

    def get_list_users_detail_week_month(self, start_time, end_time, post_name,
                                         user_email, query):
        """
        доделать изменения добавить отображение и новые данные (зацепка:
        использовать шаблон для отображения данных на клиенте)
        :param start_time: 
        :param end_time: 
        :param post_name: 
        :param user_email: 
        :param query: 
        :return: 
        """
        query = query.format(post_name, user_email, start_time, end_time)
        print('__________________________')
        print(f"{start_time} | {end_time}")
        print('__________________________')

        qrt = self.engine.execute(query).all()

        anchor = 0
        temp_list = []
        my_dicts = []
        print(f'{qrt} - This')
        for idx in range(len(qrt)):
            if anchor == qrt[idx][12]:
                print('id_temp equal bypass_id')
                my_dicts[str(qrt[idx][9])] = qrt[idx][2]
                my_dicts[str(qrt[idx][9]) + '_rank'] = 0 if qrt[idx][
                                                                3] is None else float(
                    qrt[idx][3])
                print(my_dicts, 'my_dicts')
            if not anchor:
                print('no email_temp')
                my_dicts = {
                    'id': str(time()),
                    'email': qrt[idx][0],
                    'post_name': qrt[idx][1],
                    str(qrt[idx][9]): qrt[idx][2],
                    str(qrt[idx][9]) + '_rank': 0 if qrt[idx][
                                                         3] is None else float(
                        qrt[idx][3]),
                    'avg_rank': float(qrt[idx][4]),
                    'time_bypass': int(qrt[idx][5]),
                    'count_bypass': qrt[idx][6],
                    'cleaner': qrt[idx][7],
                    'time_bbb': None if qrt[idx][8] is None else int(
                        qrt[idx][8]),
                    'post_id': qrt[idx][10],
                    'date': qrt[idx][12],
                    'temperature': int(qrt[idx][11]),
                    'title': f'{qrt[idx][13]} {qrt[idx][14]} {qrt[idx][15]}'
                }
                anchor = qrt[idx][12]
            if anchor != qrt[idx][12] and anchor or (len(qrt) - 1 == idx):
                print('id_temp not equal bypass_id and not id_temp')
                temp_list.append(my_dicts)
                my_dicts = {
                    'id': str(time()),
                    'email': qrt[idx][0],
                    'post_name': qrt[idx][1],
                    str(qrt[idx][9]): qrt[idx][2],
                    str(qrt[idx][9]) + '_rank': None if qrt[idx][
                                                            3] is None else float(
                        qrt[idx][3]),
                    'avg_rank': float(qrt[idx][4]),
                    'time_bypass': int(qrt[idx][5]),
                    'count_bypass': qrt[idx][6],
                    'cleaner': qrt[idx][7],
                    'time_bbb': None if qrt[idx][8] is None else int(
                        qrt[idx][8]),
                    'post_id': qrt[idx][10],
                    'date': qrt[idx][12],
                    'temperature': int(qrt[idx][11]),
                    'title': f'{qrt[idx][13]} {qrt[idx][14]} {qrt[idx][15]}'
                }
                print(my_dicts)
                if idx == len(qrt) - 1 and qrt[idx][12] != qrt[idx - 1][12]:
                    temp_list.append(my_dicts)
            anchor = qrt[idx][12]
            with open('text-test.txt', 'w', encoding='utf-8') as f:
                f.write(str(temp_list))
        return temp_list

    def get_list_users_detail(self, start_time, end_time, post_name, user_email, query):
        """
        
        :param start_time: 
        :param end_time: 
        :param post_name: 
        :param user_email: 
        :param query: 
        :return: 
        """
        qrt = self.create_remove_view_detail(USERS_DETAIL_LIST_VIEW.
                                             format(post_name, user_email,
                                                    start_time, end_time),
                                             query)
        print(f'{post_name} {user_email} EMAIL')
        id_temp = 0
        temp_list = []
        my_dicts = []
        print(f'{qrt} - This')
        for idx in range(len(qrt)):
            if id_temp == qrt[idx][0]:
                print('id_temp equal bypass_id')
                my_dicts[str(qrt[idx][8])] = qrt[idx][9]
                my_dicts[str(qrt[idx][8]) + '_rank'] = qrt[idx][12]
                my_dicts[str(qrt[idx][8]) + '_description'] = qrt[idx][10]
                my_dicts[str(qrt[idx][8]) + '_name_c_r'] = qrt[idx][13]
                my_dicts[str(qrt[idx][8]) + '_is_image'] = qrt[idx][20]
                my_dicts[str(qrt[idx][8]) + '_bypass_rank_id'] = qrt[idx][7]
                print(my_dicts, 'my_dicts')
            if not id_temp:
                print('no id_temp')
                my_dicts = {
                    'bypass_id': qrt[idx][0],
                    'user_id': qrt[idx][1],
                    'start_time': qrt[idx][2],
                    'end_time': qrt[idx][3],
                    'weather': qrt[idx][4],
                    'temperature': qrt[idx][5],
                    'cleaner': qrt[idx][6],
                    str(qrt[idx][8]): qrt[idx][9],
                    str(qrt[idx][8]) + '_rank': qrt[idx][12],
                    str(qrt[idx][8]) + '_description': qrt[idx][10],
                    str(qrt[idx][8]) + '_name_c_r': qrt[idx][13],
                    str(qrt[idx][8]) + '_is_image': qrt[idx][20],
                    str(qrt[idx][8]) + '_bypass_rank_id': qrt[idx][7],
                    'post_name': qrt[idx][15],
                    'icon': qrt[idx][16],
                    'title': f'{qrt[idx][17]} {qrt[idx][18]} {qrt[idx][19]}',
                    'email': qrt[idx][14]
                }
                id_temp = qrt[idx][0]
            if (id_temp != qrt[idx][0] and id_temp) or (len(qrt) - 1 == idx):
                print('id_temp not equal bypass_id and not id_temp')
                temp_list.append(my_dicts)
                my_dicts = {
                    'bypass_id': qrt[idx][0],
                    'user_id': qrt[idx][1],
                    'start_time': qrt[idx][2],
                    'end_time': qrt[idx][3],
                    'weather': qrt[idx][4],
                    'temperature': qrt[idx][5],
                    'cleaner': qrt[idx][6],
                    str(qrt[idx][8]): qrt[idx][9],
                    str(qrt[idx][8]) + '_rank': qrt[idx][12],
                    str(qrt[idx][8]) + '_description': qrt[idx][10],
                    str(qrt[idx][8]) + '_name_c_r': qrt[idx][13],
                    str(qrt[idx][8]) + '_is_image': qrt[idx][20],
                    str(qrt[idx][8]) + '_bypass_rank_id': qrt[idx][7],
                    'post_name': qrt[idx][15],
                    'icon': qrt[idx][16],
                    'title': f'{qrt[idx][17]} {qrt[idx][18]} {qrt[idx][19]}',
                    'email': qrt[idx][14],
                    'is_image': qrt[idx][20]
                }
                print(my_dicts)
                if idx == len(qrt) - 1 and qrt[idx][0] != qrt[idx - 1][0]:
                    temp_list.append(my_dicts)
            id_temp = qrt[idx][0]

        return temp_list

    def get_list_users(self, is_time, post_name) -> list:
        """

        :param is_time:
        :param post_name:
        :return:
        """
        qrt = self.create_and_remove_view_and_get_list(is_time,
                                                       USERS_LIST_VIEW,
                                                       QUERY_GET_USERS.format(
                                                           f"'{post_name}'"))
        print(is_time, 'TIme')
        print([el for el in qrt])
        return [{
            'id': str(time() + random.randint(1, 18000)),
            'title': f'{el[0]} {el[1]} {el[2]}',
            'bestComponent': el[5],
            'bestComponentRank': self._to_fixed(el[6], 1),
            'badComponent': el[7],
            'badComponentRank': self._to_fixed(el[8], 1),
            'countTime': int(el[12]),
            'countBypass': el[11],
            'avgRanks': self._to_fixed(el[10], 1),
            'steps': '-',
            'trand': 1,
            'email': el[13],
            'post_name': el[4]
        }
            for el in qrt]

    def get_status_posts(self, object_name, period) -> list:
        """

        :param object_name:
        :param period:
        :return:
        """
        if period == 'today':
            return self.get_list_posts(TODAY_MILLISECONDS, object_name)
        if period == 'week':
            return self.get_list_posts(WEEK_MILLISECONDS, object_name)
        if period == 'month':
            return self.get_list_posts(MONTH_MILLISECONDS, object_name)
        if period == 'year':
            return self.get_list_posts(YEAR_MILLISECONDS, object_name)

    def get_list_posts(self, is_time, object_name) -> list:
        """

        :param is_time:
        :param object_name:
        :return:
        """
        qrt = self.create_remove_view_detail(
            POSTS_LIST_VIEW.format(get_today() -
                                   is_time, get_today() +
                                   TAIL_TODAY),
            QUERY_GET_POSTS.format(
                get_today() -
                is_time,
                f"'{object_name}'"))
                                                       
        print([el for el in qrt])
        return [{
            'id': str(time() + random.randint(1, 15000)),
            'title': el[2],
            'bestComponent': el[3],
            'bestComponentRank': self._to_fixed(el[4], 1),
            'badComponent': el[5],
            'badComponentRank': self._to_fixed(el[6], 1),
            'countTime': el[7],
            'countBypass': el[8],
            'avgRanks': self._to_fixed(el[9], 1),
            'countUsers': el[10],
            'steps': '-',
            'trand': 1
        }
            for el in qrt]

    def get_list_objects(self, is_time) -> list:
        """

        :param is_time:
        :return:
        """
        qrt = self.engine.execute(QUERY_GET_OBJECTS.
                                  format(get_today() - is_time,
                                         get_today() + TAIL_TODAY)).all()

        return [{
            'id': str(time() + random.randint(1, 15000)),
            'title': el[1],
            'avgRanks': self._to_fixed(el[8], 1),
            'countBypass': el[7],
            'countTime': el[6],
            'bestPost': el[2],
            'bestRank': self._to_fixed(el[3], 1),
            'badPost': el[4],
            'badRank': self._to_fixed(el[5], 1),
            'countCircle': '-',
            'steps': '-',
            'trand': 1,
            'building_id': el[0],
            'cycle': el[9],
            'time_between_bypass': el[10]
        }
            for el in qrt]

    def get_status_object(self, period) -> list:
        """

        :param period:
        :return:
        """
        if period == 'today':
            return self.get_list_objects(TODAY_MILLISECONDS)
        if period == 'week':
            return self.get_list_objects(WEEK_MILLISECONDS)
        if period == 'month':
            return self.get_list_objects(MONTH_MILLISECONDS)
        if period == 'year':
            return self.get_list_objects(YEAR_MILLISECONDS)

    def get_list_objects_detail(self, is_time, object_name):
        """
        
        :param is_time: 
        :param object_name: 
        :return: 
        """
        qrt = self.create_remove_view_detail(OBJECT_DETAIL_LIST_VIEW.format(
            round(get_today()) - is_time, f"'{object_name}'",
            get_today() + TAIL_TODAY),
            QUERY_OBJECT_DETAIL_LIST)
        return [{
            'id': str(time() + random.randint(1, 15000)),
            'object_name': el[0],
            'post_name': el[1],
            'surname': el[2],
            'first_name': el[3],
            'lastname': el[4],
            'email': el[5],
            'weather': el[6],
            'temperature': int(el[7]),
            'cleaner': el[8],
            'icon': el[9],
            'avg_rank': float(el[10]),
            'start_time': int(el[11]),
            'end_time': int(el[12])
        }
            for el in qrt]

    def get_status_object_detail(self, period, object_name):
        """
        
        :param period: 
        :param object_name: 
        :return: 
        """
        if period == 'today':
            return self.get_list_objects_detail(TODAY_MILLISECONDS, object_name)
        if period == 'week':
            return self.get_list_objects_detail(WEEK_MILLISECONDS, object_name)
        if period == 'month':
            return self.get_list_objects_detail(MONTH_MILLISECONDS, object_name)
        if period == 'year':
            return self.get_list_objects_detail(YEAR_MILLISECONDS, object_name)

    def get_list_user_basic(self, user, start_time, end_time) -> list:
        """

        :param user:
        :param start_time:
        :param end_time:
        :return: list
        """
        user_str = f' and u.id={user}' if user else ''
        start_time_str = start_time if start_time else get_today()
        end_time_str = end_time + 86400000 if end_time else get_today() + 86400000
        
        qrt = self.engine.execute(
            QUERY_GET_BASE_STATIC_BY_USER.format(
                                                 start_time_str,
                                                 end_time_str))

        qrt_prev = self.engine.execute(
            QUERY_GET_BASE_STATIC_BY_USER.format(
                get_today(),
                get_today() + 86400000
        ))
        current_statistics = [
            {
                'id': el.id,
                'avg_rank': float(el.avg_rank) if el.avg_rank else 0,
                'count_bypass': el.count_bypass if el.count_bypass else 0,
                'cycle': el.cycle if el.cycle else 0,
                'time_between_bypass': int(el.tbr) if el.tbr else None,
                'time_bypass': int(el.time_bypass)
            }
            for el in qrt.all()]
        prev_statistics = [
            {
                'id': el.id,
                'prev_avg_rank': float(el.avg_rank) if el.avg_rank else 0,
                'prev_count_bypass': el.count_bypass if el.count_bypass else 0,
                'prev_cycle': el.cycle if el.cycle else 0,
                'prev_time_between_bypass': int(el.tbr) if el.tbr else None,
                'prev_time_bypass': int(el.time_bypass)
            }
            for el in qrt_prev.all()]
        tmp_dict = {}
        storage_to_transfer = []
        list_of_ids = {}
        global_list_stat = [*current_statistics, *prev_statistics]
        for i in range(len(global_list_stat)):
            if str(global_list_stat[i]['id']) in list_of_ids:
                storage_to_transfer[
                    list_of_ids.get(str(global_list_stat[i]['id']))].update(
                    global_list_stat[i])
            else:
                storage_to_transfer.append(global_list_stat[i])
                list_of_ids[str(global_list_stat[i]['id'])] = len(
                    storage_to_transfer) - 1

        return storage_to_transfer

    def get_status_user_basic(self, user=None, start_time=None, end_time=None):
        return self.get_list_user_basic(user, start_time, end_time)

    def get_list_user_with_tbr(self, building_id, start_time=None, end_time=None) -> list:
        """
        :param building_id:
        :param start_time:
        :param end_time:
        :return: list
        """
        start_time_str = start_time if start_time else get_today()
        end_time_str = end_time + 86400000 if end_time else get_today() + 86400000

        qrt = self.engine.execute(
            QUERY_GET_STATIC_BY_USER_WITH_TBR.format(
                start_time_str,
                end_time_str,
                building_id))

        current_statistics = [
            {
                'id': el.id,
                'avg_rank': float(el.avg_rank),
                'count_bypass': el.count_bypass,
                'cycle': el.cycle,
                'time_between_bypass': int(el.tbr) if el.tbr else None,
                'time_bypass': int(el.time_bypass),
                'building_id': building_id,
                'surname': el.surname,
                'first_name': el.first_name,
                'lastname': el.lastname,
                'trand': 1
            }
            for el in qrt.all()]
        print(f'this is tbr {current_statistics}')
        return current_statistics
        
    def get_status_user_with_tbr(self, period=None, building_id=None, 
                                 start_time=None, end_time=None):
        if period == 'today':
            pass
        if period == 'week':
            start_time = get_today() - WEEK_MILLISECONDS
        if period == 'month':
            start_time = get_today() - MONTH_MILLISECONDS
        if period == 'year':
            start_time = get_today() - YEAR_MILLISECONDS
            
        return self.get_list_user_with_tbr(building_id, start_time, end_time)

    def get_list_component_with_building(self, building_id, start_time,
                                         end_time):
        start_time_str = start_time if start_time else get_today()
        end_time_str = end_time + 86400000 if end_time else get_today() + 86400000

        qrt = self.engine.execute(
            QUERY_GET_COMPONENT_FOR_BUILDING.format(
                start_time_str,
                end_time_str,
                building_id))

        current_statistics = [
            {
                'id': el.component_id,
                'avg_rank': float(el.avg_rank),
                'count_bypass': el.count_bypass_rank,
                'time_bypasses_component': int(el.time_bypasses_component),
                'building_id': building_id,
                'component_name': el.component_name,
                'avg_rank_component': float(el.avg_rank_component),
                'sum_time_bypasses_component': int(
                    el.sum_time_bypasses_component),
                'sum_qr_scan_component': int(el.sum_qr_scan_component),
                'trand': 1
            }
            for el in qrt.all()]
        return current_statistics
    
    def get_status_component_with_building(self, period=None, building_id=None,
                                           start_time=None, end_time=None):
        if period == 'today':
            pass
        if period == 'week':
            start_time = get_today() - WEEK_MILLISECONDS
        if period == 'month':
            start_time = get_today() - MONTH_MILLISECONDS
        if period == 'year':
            start_time = get_today() - YEAR_MILLISECONDS
        
        return self.get_list_component_with_building(building_id, start_time,
                                                     end_time)
    
    def get_list_user_with_tbr_detail(self,
                                      user_id=None,
                                      building_id=None,
                                      start_time=None,
                                      end_time=None):

        start_time_str = start_time if start_time else get_today()
        end_time_str = end_time + 86400000 if end_time else get_today() + 86400000

        qrt = self.engine.execute(QUERY_GET_STATIC_BY_USER_WITH_TBR_DETAIL.
                                  format(user_id, building_id, start_time_str,
                                         end_time_str)).all()
        print(f'{user_id} {building_id} EMAIL')
        user_id_temp = 0
        post_id_temp = 0
        temp_list = []
        my_dicts = []
        print(f'{qrt} - This')
        for idx in range(len(qrt)):
            if post_id_temp == qrt[idx][2]:
                print('user_id_temp equal user_id & post_id_temp equal post_id')
                my_dicts[str(qrt[idx][3])] = qrt[idx][9]
                my_dicts[str(qrt[idx][3]) + '_rank'] = float(qrt[idx][11])
                my_dicts[str(qrt[idx][3]) + 'count_bad_rank'] = float(qrt[idx][17])
                print(my_dicts, 'my_dicts')
            if not post_id_temp:
                print('no id_temp')
                my_dicts = {
                    'user_id': qrt[idx][0],
                    'building_id': qrt[idx][1],
                    'post_id': qrt[idx][2],
                    'component_id': qrt[idx][3],
                    'surname': qrt[idx][4],
                    'first_name': qrt[idx][5],
                    'lastname': qrt[idx][6],
                    'email': qrt[idx][7],
                    'post_name': qrt[idx][8],
                    str(qrt[idx][3]): qrt[idx][9],
                    str(qrt[idx][3]) + '_rank': float(qrt[idx][11]),
                    str(qrt[idx][3]) + 'count_bad_rank': qrt[idx][17],
                    'avg_rank_post': float(qrt[idx][10]),
                    'count_bypass': int(qrt[idx][12]),
                    'cleaner': qrt[idx][13],
                    'time_bypasses': int(qrt[idx][14]),
                    'avg_bp_by_bp': float(qrt[idx][15]),
                    'avg_temperature': int(qrt[idx][16]),
                    'title': f'{qrt[idx][4]} {qrt[idx][5]} {qrt[idx][6]}'
                }
                user_id_temp = qrt[idx][0]
                post_id_temp = qrt[idx][2]
            if (post_id_temp != qrt[idx][2] and post_id_temp) or (len(qrt) - 1 == idx):
                print('id_temp not equal bypass_id and not id_temp')
                temp_list.append(my_dicts)
                my_dicts = {
                    'user_id': qrt[idx][0],
                    'building_id': qrt[idx][1],
                    'post_id': qrt[idx][2],
                    'component_id': qrt[idx][3],
                    'surname': qrt[idx][4],
                    'first_name': qrt[idx][5],
                    'lastname': qrt[idx][6],
                    'email': qrt[idx][7],
                    'post_name': qrt[idx][8],
                    str(qrt[idx][3]): qrt[idx][9],
                    str(qrt[idx][3]) + '_rank': float(qrt[idx][11]),
                    str(qrt[idx][3]) + 'count_bad_rank': qrt[idx][17],
                    'avg_rank_post': float(qrt[idx][10]),
                    'count_bypass': int(qrt[idx][12]),
                    'cleaner': qrt[idx][13],
                    'time_bypasses': int(qrt[idx][14]),
                    'avg_bp_by_bp': float(qrt[idx][15]),
                    'avg_temperature': int(qrt[idx][16]),
                    'title': f'{qrt[idx][4]} {qrt[idx][5]} {qrt[idx][6]}'
                }
                print(my_dicts)
                if idx == len(qrt) - 1 and qrt[idx][0] != qrt[idx - 1][0]:
                    temp_list.append(my_dicts)
            user_id_temp = qrt[idx][0]
            post_id_temp = qrt[idx][2]

        return temp_list

    def get_status_user_with_tbr_detail(self, period, user_id=None,
                                        building_id=None, start_time=None,
                                        end_time=None):
        if period == 'today':
            pass
        if period == 'week':
            start_time = get_today() - WEEK_MILLISECONDS
        if period == 'month':
            start_time = get_today() - MONTH_MILLISECONDS
        if period == 'year':
            start_time = get_today() - YEAR_MILLISECONDS

        return self.get_list_user_with_tbr_detail(user_id, building_id, start_time, end_time)


if __name__ == '__main__':
    server = MainDataBase()
    # test = server.is_cleaner_on_bypass('1', 1625934870036)
    # print(test)
    # test = server.get_status_user_basic()
    # print(test)
    import json
    # print(json.dumps(test, indent=4, ensure_ascii=False))
    # print(get_today())
    print(server.get_status_users_detail('month', f"'бокс'", f"'boris12343@inbox.ru'"))
    # test = server.get_status_posts_detail("'мрт'", 'week')
    # print(json.dumps(test, indent=4, ensure_ascii=False))
    # server.create_component('Обои', 'ktkt', 'C://ppgd')
    # server.create_component('Кресла', 'ktkt', 'C://ppgd')
    # server.create_component('Витрины', 'ktkt', 'C://ppgd')
    # server.create_building('Внешняя территория', 'kukushka', 'klass', 'C"//objects')
    # server.create_post(1, 'Мясной ряд', 'Rtr', 'c://objects//posts', 'gf')
    # server.create_component_to_post_link(post_id=1, component_id=1)
    # server.create_component_to_post_link(post_id=1, component_id=2)
    # a = server.get_component_to_post_links(1)
    # test = list()
    # js_dict = {'action': 'component_list'}
    # for el in a:
    #     test.append(
    #         {'name': el.name, 'description': el.description, 'img': el.image})
    # import json
    #
    # js_dict['items'] = test
    # server.session.close()
    # view = Table('bypass_ugolek_year', MetaData(), schema='main')
    # definition = text(
    #     f'select b.id, building.name, p.name, AVG(cr.rank) rank, '
    #     f'SUM(DISTINCT b.end_time - b.start_time) time_bypasses, '
    #     f'COUNT(DISTINCT  b.id) count_bypass from building '
    #     f'left join post p on building.id = p.building_id '
    #     f'left join bypass b on p.id = b.post_id '
    #     f'left join bypass_rank br on b.id = br.bypass_id '
    #     f'left join component_rank cr '
    #     f'on br.component_rank_id = cr.id '
    #     f'left join component c on br.component_id = c.id '
    #     f'left join user u on u.id = b.user_id '
    #     f'WHERE b.finished=1 and b.end_time > 1589209779311 '
    #     f'GROUP BY p.id;')
    # create_view = CreateView(view, definition)
    # my_query = str(create_view.compile()).strip()
    # server.get_status_object(my_query)
    from weakref import WeakKeyDictionary

    class IntDescriptor:
        def __set_name__(self, owner, name):
            self.name = name

        def __set__(self, instance, value):
            if not isinstance(value, str):
                raise ValueError(f'{self.name} must be a string but {type(value).__name__} was passed')
            instance.__dict__[self.name] = value

        def __get__(self, instance, owner):
            if instance is None:
                return self

            return instance.__dict__.get(self.name, None)

    class Vector:
        x = IntDescriptor()
        y = IntDescriptor()

    v = Vector()
    v.x = '5'
    print(v.x)
    print(Vector.x)
    v1 = Vector()
    v1.x = '100'
    print(v.x)
    print(v.__dict__)
    print(v1.__dict__)
    print('______________')
    my_dict = [('by', 'Code', 'Lermontov', 'Речной бокс', 'постоковский', 'Смесители', 4.17, 'Двери', 2.5, 3.3349999999999995, 2, 15196)]
    print('T+++++T')
    print(get_today() - MONTH_MILLISECONDS)
    print(get_today())

    print('----------')
    end_time = get_today() + TAIL_TODAY
    # print(
    #     json.dumps(server.get_list_users_average_for_post(1629752400000, end_time, 'мрт'), indent=4, ensure_ascii=False))
    # server.engine.execute(USERS_LIST_QUERY_AVG.format('мрт')).all()
    print(server.session.execute('select now();').all())
    # print(server.get_photo_user_of_post('month',
    #                                     1625706704854,
    #                                     1625941962224,
    #                                     r"'borisostroumov@gmail.com'",
    #                                     1,
    #                                     0))
    # print(server.get_photo_rank_gallery_count_user_post('month',
    #                                                     1625706704854,
    #                                                     1625941962224,
    #                                                     r"'borisostroumov@gmail.com'"))
    server._create_default_user()
