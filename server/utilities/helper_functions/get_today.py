from time import time


def get_today() -> int:
    """
    
    :param: 
    :return int: 
    """
    return (int(time() * 1000) - int(time() * 1000) % (24 * 60 * 60 * 1000) - (3 * 60 * 60 * 1000))
