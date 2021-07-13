OBJECTS_LIST_VIEW = f'select (select id from bypass limit 1), building.name as build_name, p.name as post_name, AVG(cr.rank::decimal) rank, '\
        f'SUM(DISTINCT b.end_time::bigint - b.start_time::bigint) time_bypasses, '\
        f'COUNT(DISTINCT  b.id) count_bypass from building '\
        f'left join post p on building.id = p.building_id '\
        f'left join bypass b on p.id = b.post_id '\
        f'left join bypass_rank br on b.id = br.bypass_id '\
        f'left join component_rank cr '\
        f'on br.component_rank_id = cr.id '\
        f'left join component c on br.component_id = c.id '\
        f'left join public.user u on u.id = b.user_id '\
        f'WHERE b.finished=1 and b.end_time::bigint > %s '\
        f'GROUP BY p.id, build_name;'

POSTS_LIST_VIEW = 'select (select bypass.id from bypass limit 1) ident, building.name  building_name, p.name as post_name, c.name as component_name, AVG(cr.rank::decimal) rank, '\
        '(select distinct SUM(DISTINCT bypass.end_time::bigint - bypass.start_time::bigint) from bypass '\
        'left join post on post.id = bypass.post_id where post.name = p.name and bypass.end_time::bigint > {0}) time_bypasses, '\
        '(select distinct count(distinct br2.bypass_id) from bypass_rank br2 '\
        'left join bypass on bypass.id = br2.bypass_id left join post on post.id = bypass.post_id where post.name = p.name and bypass.end_time::bigint > {0}) count_bypass from building '\
        'left join post p on building.id = p.building_id '\
        'left join bypass b on p.id = b.post_id '\
        'left join bypass_rank br on b.id = br.bypass_id '\
        'left join component_rank cr '\
        'on br.component_rank_id = cr.id '\
        'left join component c on br.component_id = c.id '\
        'left join public.user u on u.id = b.user_id '\
        'WHERE b.finished=1 and b.end_time::bigint > {0} '\
        'GROUP BY p.id, c.id, building_name;'

USERS_LIST_VIEW = f"select p.id post_id, u.surname, u.name, " \
    f"u.lastname, b.id, building.name building_name, p.name post_name, c.name component_name, " \
    f"AVG(cr.rank::decimal) rank, SUM(DISTINCT b.end_time::bigint - b.start_time::bigint) time_bypasses, " \
    f"COUNT(DISTINCT  b.id) count_bypass, u.email as email from building left join post p " \
    f"on building.id = p.building_id left join bypass b on p.id = b.post_id " \
    f"left join bypass_rank br on b.id = br.bypass_id left join component_rank " \
    f"cr on br.component_rank_id = cr.id left join component c " \
    f"on br.component_id = c.id left join public.user u on b.user_id = u.id " \
    f"WHERE b.finished=1 and b.end_time::bigint > %d " \
    f"GROUP BY u.id, p.id, c.id, b.id, building_name;"

POSTS_DETAIL_LIST_VIEW = "select b.id bypass_id, b2.name building_name, p.id post_id, p.name post_name, u.id user_id, u.surname surname, u.name first_name, u.lastname lastname, u.email, u.start_shift, weather, temperature, cleaner, icon, "\
"c.name component_name, c.description description, br.id bypass_rank_id, cr.rank component_rank, cr.name component_rank_name, b.start_time, b.end_time "\
"from bypass b "\
"left join post p on p.id = b.post_id "\
"left join building b2 on b2.id = p.building_id "\
"left join \"user\" u on  u.id = b.user_id "\
"left join bypass_rank br on br.bypass_id = b.id "\
"left join component_rank cr on cr.id = br.component_rank_id "\
"left join component c on c.id = br.component_id "\
"where p.name = {0} and b.end_time::bigint > {1} and b.end_time::bigint < {2};"

USERS_DETAIL_LIST_VIEW = "select bypass.id bypass_id, bypass.user_id user_id, bypass.start_time bypass_start_time, bypass.end_time bypass_end_time, bypass.weather bypass_weather, bypass.temperature bypass_temperature, bypass.cleaner bypass_cleaner, br.id bypass_rank_id, br.component_id bypass_rank_component_id, c.name component_name, c.description component_description, br.component_rank_id bypass_rank_component_rank_id, cr.rank component_rank_rank, cr.name component_rank_name, u.email user_email, p.name post_name, bypass.icon bypass_icon, u.surname user_surname, u.name user_name, u.lastname user_lastaname " \
                         "from bypass " \
                         "left join public.user u on bypass.user_id = u.id " \
                         "left join post p on bypass.post_id = p.id " \
                         "left join bypass_rank br on bypass.id = br.bypass_id " \
                         "left join component c on br.component_id = c.id " \
                         "left join component_rank cr " \
                         "on br.component_rank_id = cr.id " \
                         "where p.name = {0} and u.email = {1} " \
                         "and bypass.finished =1 " \
                         "and bypass.end_time::bigint > {2} " \
                         "and bypass.end_time::bigint < {3};"

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
    "left join public.user u2 on bypass2.user_id = u2.id " \
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
    "left join public.user u on bypasses.user_id = u.id " \
    "left join post p on bypasses.post_id = p.id " \
    "where p.name = {0} " \
    "and u.email = {1} " \
    "and bypasses.finished = 1 " \
    "and bypasses.end_time > {2} " \
    "and bypasses.end_time < {3} " \
    "group by timeses, bypasses.post_id) as testing " \
    "left join public.user u on bypass.user_id = u.id " \
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

OBJECT_DETAIL_LIST_VIEW = "select bypass.id bypass_id, bypass.user_id user_id, bypass.post_id post_id, bypass.start_time start_time, bypass.end_time end_time, bypass.weather weather, bypass.temperature temperature, bypass.cleaner cleaner, bypass.icon icon, u.surname surname, u.name as first_name, u.lastname lastname, u.email email, p.name post_name, b.name building_name, cr.rank " \
 	"from bypass " \
 	"left join public.user u on bypass.user_id = u.id "\
 	"left join post p on p.id = bypass.post_id "\
 	"left join building b on b.id = p.building_id "\
 	"left join bypass_rank br on br.bypass_id = bypass.id "\
 	"left join component_rank cr on cr.id = br.component_rank_id "\
 	"where b.name = {1} and bypass.finished = 1 and bypass.end_time::bigint > {0};"

QUERY_GET_OBJECTS = f'select (select id from public.temporary_view limit 1) as id, build_name, (select b5.post_name ' \
f'from public.temporary_view b5 ' \
f'where b5.rank = ' \
f'(select max(rank) from public.temporary_view b5 where b1.build_name = b5.build_name) and b1.build_name = b5.build_name limit 1) as ' \
            f'best_post, (select max(rank) from public.temporary_view tv where b1.build_name = tv.build_name limit 1) best_rank,' \
            f'(select b2.post_name '\
            f'from public.temporary_view b2 where b2.rank = '\
            f'(select min(rank) from public.temporary_view b2 where b1.build_name = b2.build_name) and b1.build_name = b2.build_name limit 1) '\
            f'as bad_post, (select min(rank) '\
            f'from public.temporary_view b2 where b1.build_name = b2.build_name limit 1) bad_rank, '\
            f'SUM(time_bypasses::integer) times, SUM(count_bypass::integer) '\
            f'count, AVG(rank) avg_rank from public.temporary_view b1 group by build_name;'

QUERY_GET_POSTS = "SELECT (select ident from public.temporary_view_detail limit 1), (select building_name from public.temporary_view_detail limit 1), b1.post_name post_name, " \
            "(select component_name from public.temporary_view_detail b3 where b3.rank = (" \
            "select max(rank) from public.temporary_view_detail b2 " \
            "where b1.post_name = b2.post_name and b2.component_name = component_name) limit 1) as " \
            "best_component_name, MAX(rank) best_rank_component, " \
            "(select b2.component_name from public.temporary_view_detail b2 where b2.rank = " \
            "(select min(rank) bad_component_rank from public.temporary_view_detail b2 " \
            "where b1.post_name = b2.post_name and b2.component_name = component_name ) limit 1) bad_component_name, " \
            "(select min(rank) bad_component_rank from public.temporary_view_detail b2 " \
            "where b1.post_name = b2.post_name limit 1) bad_component_rank, " \
            "sum(distinct time_bypasses::integer) as times, sum(distinct count_bypass::integer) as count,  (select avg(component_rank.rank::decimal) from bypass "\
			"left join bypass_rank br on bypass.id = br.bypass_id "\
			"left join post on bypass.post_id = post.id "\
			"left join building b on b.id = post.building_id "\
			"left join component_rank on component_rank.id = br.component_rank_id "\
			"where post.name = post_name and bypass.end_time::bigint > {0} "\
			"group by b.id, post.id limit 1) avg_rank, " \
            "(select count(distinct public.user.id) from public.bypass bypass " \
            "left join public.user on bypass.user_id = public.user.id " \
            "left join public.post post on bypass.post_id = post.id " \
            "where post.name is not null and bypass.start_time::bigint > {0} " \
            "and post.name = b1.post_name) count_users " \
            "FROM public.temporary_view_detail b1 " \
            "WHERE b1.building_name = {1} "\
            "GROUP BY post_name;"

QUERY_GET_USERS = "select surname, name as username, lastname, building_name " \
                  "as object_name, post_name,(select component_name from " \
                  "(select component_name, avg(rank) avg_rank, email from " \
                  "temporary_view tv where post_name = {0} " \
                  "group by component_name , email order by avg_rank) avgss " \
                  "where avgss.email = b3.email and avg_rank = " \
                  "(select max(avg_rank) from (select component_name, " \
                  "avg(rank) avg_rank, email from temporary_view tv " \
                  "where post_name = {0} group by component_name, " \
                  "email order by avg_rank) raps where raps.email = b3.email) limit 1) " \
                  "max_component_name,(select max(avg_rank) " \
                  "from (select component_name, avg(rank) avg_rank, email " \
                  "from temporary_view tv where post_name = {0} " \
                  "group by component_name , email order by avg_rank) avgssmin " \
                  "where avgssmin.email = b3.email) max_component_rank, " \
                  "(select component_name from (select component_name, " \
                  "avg(rank) avg_rank, email from temporary_view tv " \
                  "where post_name = {0} group by component_name , " \
                  "email order by avg_rank) avgssmin " \
                  "where avgssmin.email = b3.email and avg_rank = " \
                  "(select min(avg_rank) from (select component_name, " \
                  "avg(rank) avg_rank, email from temporary_view tv " \
                  "where post_name = {0} " \
                  "group by component_name, email order by avg_rank) rapsmin " \
                  "where rapsmin.email = b3.email) limit 1) min_component_name, " \
                  "(select min(avg_rank) from (select component_name, " \
                  "avg(rank) avg_rank, email from temporary_view tv " \
                  "where post_name = {0} group by component_name , " \
                  "email order by avg_rank) avgss " \
                  "where avgss.email = b3.email) min_component_rank, post_name, " \
                  "avg(rank), count(distinct b3.id) as count_bypass, " \
                  "sum(DISTINCT time_bypasses::bigint) as time_bypasses, email " \
                  "from public.temporary_view b3 where post_name " \
                  "like {0} group by email, surname, " \
                  "name, lastname, building_name, post_name;"

QUERY_GET_USERS_DETAIL = 'select  bypass_id, user_id, bypass_start_time, bypass_end_time, bypass_weather, bypass_temperature, bypass_cleaner, bypass_rank_id, bypass_rank_component_id, component_name, component_description, bypass_rank_component_rank_id, component_rank_rank, component_rank_name, user_email, post_name, bypass_icon, user_surname, user_name, user_lastaname ' \
                         'from public.temporary_view_detail;'

QUERY_GET_USERS_DETAIL_WEEK_MONTH = 'select id, user_id, start_time, end_time, weather, ' \
                                    'temperature, cleaner, `id:8`, ' \
                                    'component_id, `name:4`, `description:1`, ' \
                                    'component_rank_id, avg_ranked, `name:3`, ' \
                                    'email, `name:1`, icon, surname, name, ' \
                                    'lastname, times, count_bypass, avg_temperature, ' \
                                    'avg_icon, time_length from public.temporary_view_detail;'

QUERY_OBJECT_DETAIL_LIST = "select building_name, post_name, surname, first_name, lastname, email, weather, avg(temperature::integer) temperature, cleaner, icon,  avg(temporary_view_detail.rank::decimal) avg_rank, start_time, end_time "\
                             "from temporary_view_detail "\
                             "group by  bypass_id, weather, cleaner, icon, surname, first_name, lastname, email, post_name, building_name, start_time, end_time;"
QUERY_POSTS_DETAIL_LIST = "select bypass_id, building_name, post_id, post_name, user_id, surname, first_name, lastname, email, start_shift, weather, temperature, cleaner, icon, component_name, description, bypass_rank_id, component_rank, component_rank_name, start_time, end_time from temporary_view_detail;"
TODAY_MILLISECONDS = 86400000
WEEK_MILLISECONDS = 604800000
MONTH_MILLISECONDS = 2678400000
YEAR_MILLISECONDS = 31536000000
