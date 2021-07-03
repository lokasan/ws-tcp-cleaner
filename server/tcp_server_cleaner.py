import argparse
import configparser
import logging
import os
import sys
# from server.decoss.logger import loggers
# sys.path.append('../')
from server.core import Server
server_logger = logging.getLogger('server')


# @loggers
def check_arguments(default_port, default_address):
    """
    Парсинг аргументов, передаваемых из консоли
    :param default_port: 7777
    :param default_address: '127.0.0.1'
    :return: listen_address, listen_port, gui_flag
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', default=default_port, type=int, nargs='?')
    parser.add_argument('-a', default=default_address, nargs='?')
    parser.add_argument('--no_gui', action='store_true')
    namespace = parser.parse_args(sys.argv[1:])
    listen_address = namespace.a
    listen_port = namespace.p
    gui_flag = namespace.no_gui
    return listen_address, listen_port, gui_flag


def config_load():
    """
    Загрузка файлы конфигурации
    :return: экземпляр класса configparser
    """
    config = configparser.ConfigParser()
    if getattr(sys, 'frozen', False):
        # frozen
        dir_path = os.path.dirname(sys.executable)
    else:
        # unfrozen
        dir_path = os.path.dirname(os.path.realpath(__file__))
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    config.read(f"{dir_path}/{'server.ini'}")
    # Если конфиг файл загружен правильно, запускаемся, иначе конфиг по
    # умолчанию.
    if 'SETTINGS' in config:
        return config

    config.add_section('SETTINGS')
    config.set('SETTINGS', 'Default_port', str(8888))
    config.set('SETTINGS', 'Listen_Address', '')
    config.set('SETTINGS', 'Database_path', '')
    config.set('SETTINGS', 'Database_file', 'server_database.db3')
    return config


def main():
    """
    Загрузка основного модуля сервера
    :return: None
    """
    config = config_load()
    listen_address, listen_port, gui_flag = check_arguments(
        config['SETTINGS']['Default_port'], config['SETTINGS']
        ['Listen_Address'])
    # database = ServerDB(
    #     os.path.join(
    #         config['SETTINGS']['Database_path'],
    #         config['SETTINGS']['Database_file']))
    server = Server(listen_address, listen_port)
    server.start()

    while True:
        command = input('Введите exit для завершения работы сервера.')
        if command == 'exit':
            # Если выход, то завршаем основной цикл сервера.
            server.running = False
            break

    # Если не указан запуск без GUI, то запускаем GUI:
    # else:
    #     pass
        # Создаём графическое окуружение для сервера:
        # server_app = QApplication(sys.argv)
        # server_app.setAttribute(Qt.AA_DisableWindowContextHelpButton)
        # main_window = MainWindow(database, server, config)

        # Запускаем GUI
        # server_app.exec_()

        # По закрытию окон останавливаем обработчик сообщений
        # server.running = False


if __name__ == '__main__':
    main()