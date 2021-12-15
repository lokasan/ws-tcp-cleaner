from sqlalchemy.sql import text
from sqlalchemy import Table, MetaData
from sqlalchemy_views import CreateView, DropView
from time import time
from server.database.query_text.queries import *
from server.utilities.helper_functions.get_today import get_today


def view_create_detail(definition):
    view = Table('temporary_view_detail', MetaData(), schema='public')
    definition = text(definition)
    create_view = CreateView(view, definition)
    return str(create_view.compile()).strip()


def view_drop_detail():
    view = Table('temporary_view_detail', MetaData(), schema='public')
    drop_view = DropView(view)
    return str(drop_view.compile()).strip()


def view_create(remainder, definition):
    view = Table('temporary_view', MetaData(), schema='public')
    definition = text(definition % (get_today() - remainder, get_today() + TAIL_TODAY))
    create_view = CreateView(view, definition)
    return str(create_view.compile()).strip()


def view_drop():
    view = Table('temporary_view', MetaData(), schema='public')
    drop_view = DropView(view)
    return str(drop_view.compile()).strip()
