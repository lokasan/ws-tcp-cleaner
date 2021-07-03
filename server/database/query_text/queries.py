OBJECTS_LIST_VIEW = f'select b.id, building.name, p.name, AVG(cr.rank) rank, '\
        f'SUM(DISTINCT b.end_time - b.start_time) time_bypasses, '\
        f'COUNT(DISTINCT  b.id) count_bypass from building '\
        f'left join post p on building.id = p.building_id '\
        f'left join bypass b on p.id = b.post_id '\
        f'left join bypass_rank br on b.id = br.bypass_id '\
        f'left join component_rank cr '\
        f'on br.component_rank_id = cr.id '\
        f'left join component c on br.component_id = c.id '\
        f'left join user u on u.id = b.user_id '\
        f'WHERE b.finished=1 and b.end_time > %s '\
        f'GROUP BY p.id;'

POSTS_LIST_VIEW = f'select b.id, building.name, p.name, c.name, AVG(cr.rank) rank, '\
        f'SUM(DISTINCT b.end_time - b.start_time) time_bypasses, '\
        f'COUNT(DISTINCT  b.id) count_bypass from building '\
        f'left join post p on building.id = p.building_id '\
        f'left join bypass b on p.id = b.post_id '\
        f'left join bypass_rank br on b.id = br.bypass_id '\
        f'left join component_rank cr '\
        f'on br.component_rank_id = cr.id '\
        f'left join component c on br.component_id = c.id '\
        f'left join user u on u.id = b.user_id '\
        f'WHERE b.finished=1 and b.end_time > %s '\
        f'GROUP BY p.id, c.id;'

USERS_LIST_VIEW = f"select p.id, u.surname, u.name, " \
    f"u.lastname, b.id, building.name, p.name, c.name, " \
    f"AVG(cr.rank) rank, SUM(DISTINCT b.end_time - b.start_time) time_bypasses, " \
    f"COUNT(DISTINCT  b.id) count_bypass, u.email as email from building left join post p " \
    f"on building.id = p.building_id left join bypass b on p.id = b.post_id " \
    f"left join bypass_rank br on b.id = br.bypass_id left join component_rank " \
    f"cr on br.component_rank_id = cr.id left join component c " \
    f"on br.component_id = c.id left join user u on b.user_id = u.id " \
    f"WHERE b.finished=1 and b.end_time > %d " \
    f"GROUP BY u.id, p.id, c.id;"

USERS_DETAIL_LIST_VIEW = "select * " \
                         "from bypass " \
                         "left join user u on bypass.user_id = u.id " \
                         "left join post p on bypass.post_id = p.id " \
                         "left join bypass_rank br on bypass.id = br.bypass_id " \
                         "left join component c on br.component_id = c.id " \
                         "left join component_rank cr " \
                         "on br.component_rank_id = cr.id " \
                         "where p.name = {0} and u.email = {1} " \
                         "and bypass.finished =1 " \
                         "and bypass.end_time > {2} " \
                         "and bypass.end_time < {3};"

USERS_DETAIL_LIST_VIEW_WEEK_MONTH = "" \
    "select distinct " \
                                    "bypass.id, bypass.user_id, " \
                                    "bypass.cleaner, c.id, c.name, " \
                                    "c.description, avg(rank) " \
                                    "avg_ranked, u.email, " \
                                    "p.name post_name, u.surname, " \
                                    "u.name, u.lastname, " \
    "strftime('%Y-%m-%d', (bypass.end_time + 10800000) / 1000, 'unixepoch') " \
    "as times, testing.count_bypass, " \
    "avg(bypass.temperature) as avg_temperature, max(bypass.icon) as avg_icon, " \
    "(select sum(distinct bypass2.end_time - bypass2.start_time) " \
    "from bypass as bypass2 " \
    "left join post p2 on bypass2.post_id = p2.id " \
    "left join user u2 on bypass2.user_id = u2.id " \
    "left join bypass_rank b on bypass2.id = b.bypass_id " \
    "left join component c2 on b.component_id = c2.id " \
    "left join component_rank r on b.component_rank_id = r.id " \
    "where p2.name = {0} " \
    "and u2.email = {1} " \
    "and bypass2.finished =1 " \
    "and bypass2.end_time > {2} " \
    "and bypass2.end_time < {3} " \
    "and strftime('%Y-%m-%d', (bypass2.end_time + 10800000) / 1000, 'unixepoch') = testing.timeses) time_length, bypass.weather from bypass, " \
    "(select strftime('%Y-%m-%d', " \
    "(bypasses.end_time + 10800000) / 1000, 'unixepoch') as timeses, " \
    "count(bypasses.id) count_bypass, * from bypass bypasses " \
    "left join user u on bypasses.user_id = u.id " \
    "left join post p on bypasses.post_id = p.id " \
    "where p.name = {0} " \
    "and u.email = {1} " \
    "and bypasses.finished = 1 " \
    "and bypasses.end_time > {2} " \
    "and bypasses.end_time < {3} " \
    "group by timeses, bypasses.post_id) as testing " \
    "left join user u on bypass.user_id = u.id " \
    "left join post p on bypass.post_id = p.id " \
    "left join bypass_rank br on bypass.id = br.bypass_id " \
    "left join component c on br.component_id = c.id " \
    "left join component_rank cr on br.component_rank_id = cr.id " \
    "where p.name = {0} " \
    "and u.email = {1} " \
    "and bypass.finished =1 " \
    "and bypass.end_time > {2} " \
    "and bypass.end_time < {3} " \
    "and times = testing.timeses group by times, c.name;"

QUERY_GET_OBJECTS = f"select id, name, b1.'name:1' " \
            f"best_post, max(rank) best_rank, " \
            f"(select b2.'name:1' " \
            f"from public.temporary_view b2 where b2.rank = " \
            f"(select min(rank) from public.temporary_view b2 where b1.name = b2.name)) " \
            f"as bad_post, (select min(rank) " \
            f"from public.temporary_view b2 where b1.name = b2.name) bad_rank, " \
            f"SUM(time_bypasses) times, SUM(count_bypass) " \
            f"count, AVG(rank) avg_rank from public.temporary_view b1 group by name;"

QUERY_GET_POSTS = f"SELECT id, name, b1.'name:1' post_name, " \
            f"(select `name:2` from public.temporary_view b3 where b3.rank = (" \
            f"select max(rank) from public.temporary_view b2 " \
            f"where b1.'name:1' = b2.'name:1')) as " \
            f"best_component_name, MAX(rank) best_rank_component, " \
            f"(select b2.'name:2' from public.temporary_view b2 where b2.rank = " \
            f"(select min(rank) bad_component_rank from public.temporary_view b2 " \
            f"where b1.'name:1' = b2.'name:1')) bad_component_name, " \
            f"(select min(rank) bad_component_rank from public.temporary_view b2 " \
            f"where b1.'name:1' = b2.'name:1') bad_component_rank, " \
            f"max(time_bypasses) as times, max(count_bypass) as count, AVG(rank) avg_rank, " \
            f"(select count(distinct user.id) from public.bypass bypass " \
            f"left join public.user user on bypass.user_id = user.id " \
            f"left join public.post post on bypass.post_id = post.id " \
            f"where post.name is not null and bypass.start_time > %d " \
            f"and post.name = b1.'name:1') count_users " \
            f"FROM public.temporary_view b1 " \
            f"WHERE b1.name = %s "\
            f"GROUP BY post_name;"

QUERY_GET_USERS = "select surname, name as username, lastname, `name:1` " \
    "as object_name, `name:2` as post_name, (select `name:3` " \
    "from public.temporary_view b1 " \
    "where b1.rank = (select max(rank) " \
    "from public.temporary_view b2 " \
    "where b2.name = b3.name and b2.`name:2` = {0}) and b1.`name:2` = {0}) as max_component_name, " \
    "max(rank) as max_component_rank, " \
    "(select `name:3` from public.temporary_view b1 " \
    "where b1.rank = " \
    "(select min(rank) from public.temporary_view b2 " \
    "where b2.name = b3.name and b2.`name:2` = {0}) and b1.`name:2` = {0}) as bad_component_name, " \
    "(select min(rank) from public.temporary_view b1 " \
    "where b1.name = b3.name and b1.`name:2` = {0}) as bad_component_rank, " \
    "avg(rank), max(DISTINCT count_bypass) as count_bypass, sum(DISTINCT time_bypasses) as time_bypasses, email " \
    "from public.temporary_view b3 " \
    "where post_name like {0} group by username;"

QUERY_GET_USERS_DETAIL = 'select id, user_id, start_time, end_time, weather, ' \
                         'temperature, cleaner, `id:3`, component_id, `name:2`, ' \
                         '`description:1`, component_rank_id, rank, `name:3`, email, `name:1`, icon, surname, name, lastname ' \
                         'from public.temporary_view_detail;'

QUERY_GET_USERS_DETAIL_WEEK_MONTH = 'select id, user_id, start_time, end_time, weather, ' \
                                    'temperature, cleaner, `id:8`, ' \
                                    'component_id, `name:4`, `description:1`, ' \
                                    'component_rank_id, avg_ranked, `name:3`, ' \
                                    'email, `name:1`, icon, surname, name, ' \
                                    'lastname, times, count_bypass, avg_temperature, ' \
                                    'avg_icon, time_length from public.temporary_view_detail;'

TODAY_MILLISECONDS = 86400000
WEEK_MILLISECONDS = 604800000
MONTH_MILLISECONDS = 2678400000
YEAR_MILLISECONDS = 31536000000
