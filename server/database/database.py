import time

from sqlalchemy import Column, Integer, String, Text, ForeignKey, \
    create_engine, Table, MetaData, desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.sql import text
from sqlalchemy_views import CreateView
from time import time
import random
from server.database.query_text.queries import *
from server.database.query_text.view import view_create, view_drop, view_create_detail, view_drop_detail

STOP_BYPASS = 3600


class MainDataBase:
    Base = declarative_base()

    class User(Base):
        """

        """
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
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
        id = Column(Integer, primary_key=True, autoincrement=True)
        user_id = Column(String, ForeignKey('user.id'))
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
        id = Column(Integer, primary_key=True, autoincrement=True)
        user_id = Column(String, ForeignKey('user.id'))
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
        user_id = Column(Integer, ForeignKey('user.id'))
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
        id = Column(Integer, primary_key=True)
        user_id = Column(Integer, ForeignKey('user.id'))
        post_id = Column(Integer, ForeignKey('post.id'))
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
        id = Column(Integer, primary_key=True)
        bypass_id = Column(Integer, ForeignKey('bypass.id'))
        component_id = Column(Integer, ForeignKey('component.id'))
        component_rank_id = Column(Integer, ForeignKey('component_rank.id'))
        start_time = Column(Text)
        end_time = Column(Text)
        photo_rank_gallery = relationship('PhotoRankGallery',
                                          cascade="all,delete",
                                          backref='bypass_rank')

        def __init__(self, id, bypass_id, component_id, start_time):
            self.id = id
            self.bypass_id = bypass_id
            self.component_id = component_id
            self.start_time = start_time
            self.end_time = None

    class PhotoRankGallery(Base):
        """

        """
        __tablename__ = 'photo_rank_gallery'
        id = Column(Integer, primary_key=True)
        bypass_rank_id = Column(Integer, ForeignKey('bypass_rank.id'))
        image = Column(Text)

        def __init__(self, id, bypass_rank_id, image):
            self.id = id
            self.bypass_rank_id = bypass_rank_id
            self.image = image

    class Building(Base):
        """

        """
        __tablename__ = 'building'
        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False, unique=True)
        address = Column(String)
        description = Column(String)
        image = Column(Text)
        post = relationship('Post', cascade="all,delete", backref='building')

        def __init__(self, id, name, address, description, image):
            self.id = id
            self.name = name
            self.address = address
            self.description = description
            self.image = image

    class Post(Base):
        """

        """
        __tablename__ = 'post'
        id = Column(Integer, primary_key=True)
        building_id = Column(Integer, ForeignKey('building.id'))
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
        id = Column(Integer, primary_key=True)
        post_id = Column(Integer, ForeignKey('post.id'))
        component_id = Column(Integer, ForeignKey('component.id'))

        def __int__(self, id, post_id, component_id):
            self.id = id
            self.post_id = post_id
            self.component_id = component_id

    class Component(Base):
        """

        """
        __tablename__ = 'component'
        id = Column(Integer, primary_key=True)
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
        id = Column(Integer, primary_key=True)
        component_id = Column(Integer, ForeignKey('component.id'))
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
        id = Column(Integer, primary_key=True)
        user_id = Column(Integer, ForeignKey('user.id'))
        push_token = Column(Text, nullable=False)
        
        def __init__(self, user_id, push_token):
            self.user_id = user_id
            self.push_token = push_token

    class UserShift(Base):
        __tablename__ = 'user_shift'
        id = Column(Integer, primary_key=True, autoincrement=True)
        user_id = Column(Integer, ForeignKey('user.id'))
        start_shift = Column(Text, nullable=False)
        create_date = Column(Text, nullable=False)

        def __init__(self, user_id, start_shift):
            self.user_id = user_id
            self.start_shift = start_shift
            self.create_date = int(time() * 1000)

    def __init__(self, path='server_base.db3'):
        self.engine = create_engine(f'sqlite:///{path}', pool_recycle=7200,
                                    connect_args={'check_same_thread': False})
        self.Base.metadata.create_all(self.engine)
        session_factory = sessionmaker(bind=self.engine)
        Session = scoped_session(session_factory)
        self.session = Session()

        # will added cleat list function connected users
        self.session.query(self.ActiveUser).delete()
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
        active_users_row = self.session.query(self.ActiveUser).all()
        return [getattr(user, 'user_id', 0) for user in active_users_row]

    def user_login(self, user_id, ip, port, time_conn, id_ws_handler):
        user_active_from_id = self.session.query(self.ActiveUser).filter_by(id_ws=id_ws_handler).first()
        print(user_active_from_id)
        if user_active_from_id:
            user_active_from_id.user_id = user_id
        else:
            active_user_row = self.ActiveUser(user_id, ip, port, time_conn, id_ws_handler)
            self.session.add(active_user_row)
        self.session.commit()
    
    def user_critical_logout(self, id_ws_handler):
        user = self.session.query(self.ActiveUser).filter_by(
            id_ws=id_ws_handler).first()
        print('user', user)

        self.session.query(self.ActiveUser).filter_by(id_ws=id_ws_handler).delete()
        self.session.commit()
        return user

    def user_logout(self, email: str, id_ws: str) -> dict:
        """
        Method for logout user of server
        :param email: str
        :return: None
        """
        user = self.session.query(self.User).filter_by(email=email).first()
        user_count = 1
        user_active_many = self.session.query(self.ActiveUser).filter_by(user_id=user.id).all()
        if isinstance(user_active_many, list):
            user_count = len(user_active_many)
            
        user_row = self.session.query(self.ActiveUser).filter_by(user_id=user.id, id_ws=id_ws).first()
        user_id = user_row.user_id
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

    def create_user_shift(self, user_id, start_shift):
        """

        :param user_id:
        :param start_shift:
        :return:
        """
        user_sh_raw = self.UserShift(user_id, start_shift)
        user_update_row = self.session.query(self.User).filter_by(id=user_id).first()
        user_update_row.start_shift = start_shift
        self.session.add(user_sh_raw)
        self.session.commit()

    def update_user_shift(self, user_id, start_shift):
        user_shift = self.session.query(self.UserShift)\
            .filter_by(user_id=user_id)\
            .order_by(desc(self.UserShift.id))\
            .first()
        user_shift.start_shift = start_shift
        user_update_row = self.session.query(self.User).filter_by(
            id=user_id).first()
        user_update_row.start_shift = start_shift
        self.session.commit()

    def get_user_shift(self, user_id):
        user_shift = self.session.query(self.UserShift)\
            .filter_by(user_id=user_id)\
            .order_by(desc(self.UserShift.id)).first()
        print(user_shift)
        return {
            'user_id': user_shift.user_id,
            'start_shift': user_shift.start_shift,
            'create_date': user_shift.create_date
        } if user_shift else user_shift

    def remove_user(self, user_id: str) -> None:
        """

        :param email:
        :return: None
        """
        user = self.session.query(self.User).filter_by(id=user_id)
        user.delete()
        self.session.commit()

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
                'start_shift': element.start_shift
            }
            for element in users.all()]

    def create_building(self, id, name, address, description, image) -> None:
        """

        :param name:
        :param address:
        :param description:
        :param image:
        :return: None
        """
        building_row = self.Building(id, name, address, description, image)
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

    def get_buildings(self) -> list:
        """

        :return: None
        """
        buildings = self.session.query(self.Building)
        return [
            {
                'id': element.id,
                'name': element.name,
                'address': element.address,
                'description': element.description,
                'image': element.image
            }
            for element in buildings.all()]

    def get_building_id(self, id):
        building = self.session.query(self.Building).filter_by(id=id).first()
        return building

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
    
    def get_user_email(self, email: str) -> int:
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

    def get_component_to_post_links(self, post_id):
        components = self.session.query(self.Component).join(
            self.ComponentWithPost,
            self.ComponentWithPost.component_id == self.Component.id,
            isouter=True).filter_by(post_id=post_id)
        return [
            {
                'id': el.id,
                'name': el.name,
                'desctiption': el.description,
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
    def update_emploee_privileg(self, privileg, id):
        user = self.session.query(self.User).filter_by(id=id).first()
        user.privileg = privileg
        self.session.commit()

    def update_bypass_rank(self, component_rank_id, id, end_time) -> None:
        """

        :param component_rank_id:
        :param id:
        :return: None
        """
        bypass_rank = self.session.query(self.BypassRank).filter_by(
            id=id).first()
        bypass_rank.component_rank_id = component_rank_id
        bypass_rank.end_time = end_time
        self.session.commit()

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
        self.engine.execute(view_create_detail(create_view))
        qrt = self.engine.execute(query).all()
        self.engine.execute(view_drop_detail())
        return qrt

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

    def get_status_users_detail(self, period, post_name, user_email):
        if period == 'today':
            return self.get_list_users_detail(
                round(time() * 1000 - TODAY_MILLISECONDS),
                round(time() * 1000),
                post_name,
                user_email,
                QUERY_GET_USERS_DETAIL
            )

        if period == 'week':
            return self.get_list_users_detail(
                round(time() * 1000 - WEEK_MILLISECONDS),
                round(time() * 1000),
                post_name,
                user_email,
                QUERY_GET_USERS_DETAIL)
        if period == 'month':
            self.get_list_users_detail_week_month(
                round(time() * 1000 - MONTH_MILLISECONDS),
                round(time() * 1000),
                post_name,
                user_email,
                QUERY_GET_USERS_DETAIL_WEEK_MONTH
            )
            return self.get_list_users_detail(
                round(time() * 1000 - MONTH_MILLISECONDS),
                round(time() * 1000),
                post_name,
                user_email,
                QUERY_GET_USERS_DETAIL
            )
        if period == 'year':
            return self.get_list_users_detail(
                round(time() * 1000 - YEAR_MILLISECONDS),
                round(time() * 1000),
                post_name,
                user_email,
                QUERY_GET_USERS_DETAIL
            )

    def get_list_users_detail_week_month(self, start_time, end_time, post_name,
                                         user_email, query):

        qrt = self.engine.execute(USERS_DETAIL_LIST_VIEW_WEEK_MONTH.
                                  format(post_name, user_email,
                                         start_time, end_time)).all()
        id_temp = 0
        temp_list = []
        my_dicts = []
        print(f'{qrt} - This')
        for idx in range(len(qrt)):
            if id_temp == qrt[idx][0]:
                print('id_temp equal bypass_id')
                my_dicts[str(qrt[idx][3])] = qrt[idx][4]
                my_dicts[str(qrt[idx][3]) + '_rank'] = qrt[idx][6]
                my_dicts[str(qrt[idx][3]) + '_description'] = qrt[idx][5]
                print(my_dicts, 'my_dicts')
            if not id_temp:
                print('no id_temp')
                my_dicts = {
                    'bypass_id': qrt[idx][0],
                    'user_id': qrt[idx][1],
                    'cleaner': qrt[idx][2],
                    'weather': qrt[idx][17],
                    'temperature': qrt[idx][5],
                    str(qrt[idx][3]): qrt[idx][4],
                    str(qrt[idx][3]) + '_rank': qrt[idx][6],
                    str(qrt[idx][3]) + '_description': qrt[idx][5],
                    'email': qrt[idx][7],
                    'post_name': qrt[idx][8],
                    'title': f'{qrt[idx][9]} {qrt[idx][10]} {qrt[idx][11]}',
                    'date': qrt[idx][12],
                    'count_bypass': qrt[idx][13],
                    'avg_temperature': qrt[idx][14],
                    'time_length': qrt[idx][16],
                    'icon': qrt[idx][15],
                }
                id_temp = qrt[idx][0]
            if id_temp != qrt[idx][0] and id_temp or (len(qrt) - 1 == idx):
                print('id_temp not equal bypass_id and not id_temp')
                temp_list.append(my_dicts)
                my_dicts = {
                    'bypass_id': qrt[idx][0],
                    'user_id': qrt[idx][1],
                    'cleaner': qrt[idx][2],
                    'weather': qrt[idx][17],
                    'temperature': qrt[idx][5],
                    str(qrt[idx][3]): qrt[idx][4],
                    str(qrt[idx][3]) + '_rank': qrt[idx][6],
                    str(qrt[idx][3]) + '_description': qrt[idx][5],
                    'email': qrt[idx][7],
                    'post_name': qrt[idx][8],
                    'title': f'{qrt[idx][9]} {qrt[idx][10]} {qrt[idx][11]}',
                    'date': qrt[idx][12],
                    'count_bypass': qrt[idx][13],
                    'avg_temperature': qrt[idx][14],
                    'time_length': qrt[idx][16],
                    'icon': qrt[idx][15],
                }
                print(my_dicts)
            id_temp = qrt[idx][0]
            with open('text-test.txt', 'w', encoding='utf-8') as f:
                f.write(str(temp_list))
        return temp_list

    def get_list_users_detail(self, start_time, end_time, post_name, user_email, query):
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
                my_dicts[str(qrt[idx][7])] = qrt[idx][9]
                my_dicts[str(qrt[idx][7]) + '_rank'] = qrt[idx][12]
                my_dicts[str(qrt[idx][7]) + '_description'] = qrt[idx][10]
                my_dicts[str(qrt[idx][7]) + '_name_c_r'] = qrt[idx][13]
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
                    str(qrt[idx][7]): qrt[idx][9],
                    str(qrt[idx][7]) + '_rank': qrt[idx][12],
                    str(qrt[idx][7]) + '_description': qrt[idx][10],
                    str(qrt[idx][7]) + '_name_c_r': qrt[idx][13],
                    'post_name': qrt[idx][15],
                    'icon': qrt[idx][16],
                    'title': f'{qrt[idx][17]} {qrt[idx][18]} {qrt[idx][19]}',
                    'email': qrt[idx][14]
                }
                id_temp = qrt[idx][0]
            if id_temp != qrt[idx][0] and id_temp or (len(qrt) - 1 == idx):
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
                    str(qrt[idx][7]): qrt[idx][9],
                    str(qrt[idx][7]) + '_rank': qrt[idx][12],
                    str(qrt[idx][7]) + '_description': qrt[idx][10],
                    str(qrt[idx][7]) + '_name_c_r': qrt[idx][13],
                    'post_name': qrt[idx][15],
                    'icon': qrt[idx][16],
                    'title': f'{qrt[idx][17]} {qrt[idx][18]} {qrt[idx][19]}',
                    'email': qrt[idx][14]
                }
                print(my_dicts)
            id_temp = qrt[idx][0]

        return temp_list

    def get_list_users(self, is_time, post_name) -> list:
        """

        :param is_time:
        :param post_name:
        :return:
        """
        qrt = self.create_and_remove_view_and_get_list(is_time, USERS_LIST_VIEW,
                                                       QUERY_GET_USERS.format(f"'{post_name}'"))
        print(is_time, 'TIme')
        print([el for el in qrt])
        return [{
            'id': str(time() + random.randint(1, 18000)),
            'title': f'{el[0]} {el[1]}',
            'bestComponent': el[5],
            'bestComponentRank': self._to_fixed(el[6], 1),
            'badComponent': el[7],
            'badComponentRank': self._to_fixed(el[8], 1),
            'countTime': el[11],
            'countBypass': el[10],
            'avgRanks': self._to_fixed(el[9], 1),
            'steps': '-',
            'trand': 1,
            'email': el[12],
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
        qrt = self.create_and_remove_view_and_get_list(is_time,
                                                       POSTS_LIST_VIEW,
                                                       QUERY_GET_POSTS %
                                                       (round(
                                                           time() * 1000) -
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
        qrt = self.create_and_remove_view_and_get_list(is_time,
                                                       OBJECTS_LIST_VIEW,
                                                       QUERY_GET_OBJECTS)
        return [{
            'id': str(time() + random.randint(1, 15000)),
            'title': el[1],
            'avgRanks': self._to_fixed(el[-1], 1),
            'countBypass': el[7],
            'countTime': el[6],
            'bestPost': el[2],
            'bestRank': self._to_fixed(el[3], 1),
            'badPost': el[4],
            'badRank': self._to_fixed(el[5], 1),
            'countCircle': '-',
            'steps': '-',
            'trand': 1
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


if __name__ == '__main__':
    server = MainDataBase()
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


