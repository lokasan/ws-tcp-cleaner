from time import time


def get_today() -> int:
    """
    
    :param: 
    :return int: 
    """
    return (int(time() * 1000) - int(time() * 1000) % (24 * 60 * 60 * 1000) - (
                3 * 60 * 60 * 1000))


def get_start_day(start_time) -> int:
    return start_time - start_time % (24 * 60 * 60 * 1000)

