from sqlalchemy.sql import text
from sqlalchemy import Table, MetaData
from sqlalchemy_views import CreateView, DropView
from time import time


def view_create_detail(definition):
    view = Table('temporary_view_detail', MetaData(), schema='main')
    definition = text(definition)
    create_view = CreateView(view, definition)
    return str(create_view.compile()).strip()


def view_drop_detail():
    view = Table('temporary_view_detail', MetaData(), schema='main')
    drop_view = DropView(view)
    return str(drop_view.compile()).strip()


def view_create(remainder, definition):
    view = Table('temporary_view', MetaData(), schema='main')
    print(round(time() * 1000) - remainder)
    definition = text(definition % (round(time() * 1000) - remainder))
    create_view = CreateView(view, definition)
    return str(create_view.compile()).strip()


def view_drop():
    view = Table('temporary_view', MetaData(), schema='main')
    drop_view = DropView(view)
    return str(drop_view.compile()).strip()
