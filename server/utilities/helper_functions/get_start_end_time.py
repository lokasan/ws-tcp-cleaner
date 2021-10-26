from server.utilities.helper_functions.get_today import get_today, get_start_day
from server.database.query_text.queries import *


def get_start_end_time(period, start_time=None, end_time=None):
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
    if period == 'day':
        start_time_str = get_start_day(start_time)
        end_time_str = start_time_str + 86400000
    if period == 'range':
        start_time_str = get_start_day(start_time)
        end_time_str = get_start_day(end_time) + 86400000
    
    return start_time_str, end_time_str
