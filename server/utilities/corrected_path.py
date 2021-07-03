import os


class PathMaker:
    @staticmethod
    def change_path(path, file_name, key_editor):
        if os.path.exists(path.rsplit(os.sep, 1)[0]) and key_editor == 'photo':
            try:
                for file in os.listdir(path):
                    os.remove(path + os.sep + file)
            except FileNotFoundError and IsADirectoryError and PermissionError:
                pass
        os.rename(path, path.rsplit(os.sep, 1)[0] + os.sep + file_name)

    @staticmethod
    def edit_path():
        pass
