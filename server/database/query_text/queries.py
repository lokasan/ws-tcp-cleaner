OBJECTS_LIST_VIEW = f'select (select id from bypass limit 1), building.name as build_name, p.name as post_name, AVG(cr.rank::decimal) rank, '\
        f'SUM(DISTINCT b.end_time::bigint - b.start_time::bigint) time_bypasses, '\
        f'COUNT(DISTINCT  b.id) count_bypass, building.id as building_id from building '\
        f'left join post p on building.id = p.building_id '\
        f'left join bypass b on p.id = b.post_id '\
        f'left join bypass_rank br on b.id = br.bypass_id '\
        f'left join component_rank cr '\
        f'on br.component_rank_id = cr.id '\
        f'left join component c on br.component_id = c.id '\
        f'left join public.user u on u.id = b.user_id '\
        f'WHERE b.finished=1 and b.end_time::bigint >= %s and b.end_time::bigint <= %s'\
        f'GROUP BY p.id, build_name, building.id;'

POSTS_LIST_VIEW = 'select (select bypass.id from bypass limit 1) ident, building.name  building_name, p.name as post_name, c.name as component_name, AVG(cr.rank::decimal) rank, '\
        '(select distinct SUM(DISTINCT bypass.end_time::bigint - bypass.start_time::bigint) from bypass '\
        'left join post on post.id = bypass.post_id where post.name = p.name and bypass.start_time::bigint > {0} and bypass.end_time::bigint <= {1}) time_bypasses, '\
        '(select distinct count(distinct br2.bypass_id) from bypass_rank br2 '\
        'left join bypass on bypass.id = br2.bypass_id left join post on post.id = bypass.post_id where post.name = p.name and bypass.end_time::bigint > {0}) count_bypass from building '\
        'left join post p on building.id = p.building_id '\
        'left join bypass b on p.id = b.post_id '\
        'left join bypass_rank br on b.id = br.bypass_id '\
        'left join component_rank cr '\
        'on br.component_rank_id = cr.id '\
        'left join component c on br.component_id = c.id '\
        'left join public.user u on u.id = b.user_id '\
        'WHERE b.finished=1 and b.start_time::bigint > {0} and b.end_time::bigint <= {1}'\
        'GROUP BY p.id, c.id, building_name;'

USERS_LIST_VIEW = """
select distinct p.id post_id, u.surname, u.name, 
    u.lastname, b.id, building.name building_name, p.name post_name, c.name component_name, 
    AVG(cr.rank::decimal) rank, SUM(DISTINCT b.end_time::bigint - b.start_time::bigint) time_bypasses, 
    COUNT(DISTINCT  b.id) count_bypass, u.email as email, cleaner, b.start_time, b.end_time, cr.component_id, to_timestamp(b.end_time::bigint / 1000.0)::date as anchor, avg(b.temperature) temperature, max(us.create_date) over(partition by b.id) as create_date from building left join post p
    on building.id = p.building_id left join bypass b on p.id = b.post_id 
    left join bypass_rank br on b.id = br.bypass_id left join component_rank 
    cr on br.component_rank_id = cr.id left join component c 
    on br.component_id = c.id left join public.user u on b.user_id = u.id 
    left join user_shift us on us.create_date::bigint < b.start_time::bigint
    WHERE b.finished=1 and b.end_time::bigint > %d and b.end_time::bigint <= %d 
    GROUP BY u.id, p.id, c.id, b.id, building_name, cr.component_id, us.create_date
"""

USERS_LIST_VIEW_YEAR = "select p.id post_id, u.surname, u.name, " \
    "u.lastname, b.id, building.name building_name, p.name post_name, c.name component_name, " \
    "AVG(cr.rank::decimal) rank, SUM(DISTINCT b.end_time::bigint - b.start_time::bigint) time_bypasses, " \
    "COUNT(DISTINCT  b.id) count_bypass, u.email as email, cleaner, b.start_time, b.end_time, cr.component_id, substring(to_timestamp(to_timestamp(b.end_time::bigint / 1000.0)::date::text, 'YYYY-MM')::text from 3 for 5) as anchor, avg(b.temperature) temperature from building left join post p " \
    "on building.id = p.building_id left join bypass b on p.id = b.post_id " \
    "left join bypass_rank br on b.id = br.bypass_id left join component_rank " \
    "cr on br.component_rank_id = cr.id left join component c " \
    "on br.component_id = c.id left join public.user u on b.user_id = u.id " \
    "WHERE b.finished=1 and b.end_time::bigint > %d and b.end_time::bigint <= %d " \
    "GROUP BY u.id, p.id, c.id, b.id, building_name, cr.component_id;"

USERS_LIST_QUERY_AVG = """
with 
test_users_list as (select distinct p.id post_id, u.surname, u.name, 
    u.lastname, b.id, building.name building_name, p.name post_name, c.name component_name, 
    AVG(cr.rank::decimal) rank, SUM(DISTINCT b.end_time::bigint - b.start_time::bigint) time_bypasses, 
    COUNT(DISTINCT  b.id) count_bypass, u.email as email, cleaner, b.start_time, b.end_time, cr.component_id, to_timestamp(((3 * 60 * 60 * 1000) + b.end_time::bigint) / 1000.0)::date as anchor, avg(b.temperature) temperature, max(us.create_date) over(partition by b.id) as create_date from building left join post p
    on building.id = p.building_id left join bypass b on p.id = b.post_id 
    left join bypass_rank br on b.id = br.bypass_id left join component_rank 
    cr on br.component_rank_id = cr.id left join component c 
    on br.component_id = c.id left join public.user u on b.user_id = u.id 
    left join user_shift us on us.create_date::bigint < b.start_time::bigint
    WHERE b.finished=1 and b.end_time::bigint > {0} and b.end_time::bigint <= {1} 
	and p."name" = {2}
    GROUP BY u.id, p.id, c.id, b.id, building_name, cr.component_id, us.create_date),
table_main as (
select post_id, surname, "name", lastname, test_users_list.id, building_name, post_name, component_name, "rank", time_bypasses, count_bypass, email, cleaner, start_time, end_time, component_id, anchor, temperature, test_users_list.create_date, anchor + to_timestamp(us.start_shift::bigint /1000.0)::time start_shift  from test_users_list
	left join user_shift us on us.create_date = test_users_list.create_date
),
pre_help as (
	select *, abs(start_time::bigint - lag(end_time::bigint) over(partition by email, component_name, anchor order by id)) as bbb from table_main where post_name = {2}
),
table_with_start_shift as (
	select post_id, surname, "name", lastname, id, building_name, post_name, component_name, "rank", time_bypasses, count_bypass, email, cleaner, start_time, end_time, component_id, anchor, temperature, create_date, start_shift, case when bbb is null then 0 else bbb end as time_bbb from pre_help
),
output_avg_bbb as (
select post_id, surname, "name", lastname, id, building_name, post_name, component_name, avg("rank") over(partition by email) as avg_rank, avg("rank") over(partition by email, component_name) as avg_component, sum(time_bypasses) over(partition by email, component_name) as time_bypasses, count(count_bypass) over(partition by email, component_name) as count_bypass, email, (sum(cleaner::float) over(partition by email, component_name) / count(id::float) over(partition by email, component_name) * 100) as percent_cleaner, start_time, end_time, component_id, anchor, avg(temperature) over(partition by email, component_name) as temperature, create_date, start_shift, avg(time_bbb) over(partition by email) as bbb from table_with_start_shift
),
output_round_bb as (
select post_id, surname, "name", lastname, id, building_name, post_name, component_name, round(avg_rank, 1) as avg_rank, round(avg_component, 1) as avg_component, time_bypasses, count_bypass, email, round(percent_cleaner)::text || '%%' as percent_cleaner, start_time, end_time, component_id, anchor, round(temperature::numeric) as avg_temperature, create_date, start_shift, round(bbb::numeric) as time_bbb from output_avg_bbb
),
output_pre_final as (
	select
		email,
		post_name,
		component_name,
		avg_component,
		avg_rank,
		time_bypasses,
		count_bypass,
		percent_cleaner,
		time_bbb as bp_by_bp,
		component_id,
		post_id,
		count(*)
	from output_round_bb
	group by email, post_name, component_name,
		avg_component,
		avg_rank,
		time_bypasses,
		count_bypass,
		percent_cleaner,
		bp_by_bp,
		component_id,
		post_id
),
table_help as (select c.id as component_id, cr.id as component_rank_id_help, "rank" all_rank from component as c left join component_rank as cr on c.id = cr.component_id),
pre_result as (select tul.email, tul.id, tul.component_id, min(all_rank) as "rank" from test_users_list as tul left join table_help th on th.component_id = tul.component_id
and tul.id > component_rank_id_help 
group by tul.id, tul.component_id, email),
clear_best_single_rank as (select * from pre_result where "rank"::float != 5.0),
final_res as (select email, b.id, br.component_id, br.component_rank_id, cr."rank", cbsr."rank", case when cr."rank" = cbsr."rank" then 1 else 0 end as is_minimal_rank from bypass b left join bypass_rank br on br.bypass_id = b.id left join component_rank cr on br.component_rank_id = cr.id join clear_best_single_rank cbsr on cbsr.id = b.id and cbsr.component_id = br.component_id where b.finished = 1),
is_min_rank as (select email, component_id, sum(is_minimal_rank) as count_bad_rank_component from final_res group by email, component_id),
tetatet as (select email, id, cleaner from test_users_list),
tet2 as (select email, id, cleaner from tetatet group by email, id, cleaner),
calculate_data as (select test_users_list.email, count(distinct id) count_bypass, sum(distinct end_time::bigint - start_time::bigint) as time_bypasses, round(ct_bp.cleaner::numeric / count(distinct id), 2)::float * 100 || '%%' as percent_cleaner from test_users_list left join (select email, sum(cleaner) cleaner from tet2 group by email) as ct_bp on ct_bp.email = test_users_list.email group by test_users_list.email, ct_bp.cleaner)
select opf.email, post_name, component_name, avg_component, avg_rank, ct_bp.time_bypasses, ct_bp.count_bypass, ct_bp.percent_cleaner, bp_by_bp, opf.component_id, post_id, count_bad_rank_component from output_pre_final opf left join is_min_rank imr on imr.email = opf.email and opf.component_id = imr.component_id left join calculate_data as ct_bp on ct_bp.email = opf.email order by email;"""

USERS_LIST_QUERY_AVG_MONTH_WEEK = """
with table_main as (select distinct p.id post_id, u.surname, u.name, 
    u.lastname, b.id, building.name building_name, p.name post_name, c.name component_name, 
    AVG(cr.rank::decimal) rank, SUM(DISTINCT b.end_time::bigint - b.start_time::bigint) time_bypasses, 
    COUNT(DISTINCT  b.id) count_bypass, u.email as email, cleaner, b.start_time, b.end_time, cr.component_id, to_timestamp(((3 * 60 * 60 * 1000) + b.end_time::bigint) / 1000.0)::date as anchor, avg(b.temperature) temperature, max(us.create_date) over(partition by b.id) as create_date from building left join post p
    on building.id = p.building_id left join bypass b on p.id = b.post_id 
    left join bypass_rank br on b.id = br.bypass_id left join component_rank 
    cr on br.component_rank_id = cr.id left join component c 
    on br.component_id = c.id left join public.user u on b.user_id = u.id 
    left join user_shift us on us.create_date::bigint < b.start_time::bigint
    WHERE b.finished=1 and p.name = {0} and email = {1} and b.end_time::bigint > {2} and b.end_time::bigint <= {3}
    GROUP BY u.id, p.id, c.id, b.id, building_name, cr.component_id, us.create_date),
users_detail_month_week as ( 
select 
	test_table.email, 
	post_name, 
	component_name, 
	round(avg(rank), 1) avg_component, 
	round(avg_rank, 1) avg_rank, 
	t_v2.time_bypasses, 
	t_v2.count_bypass, 
	is_cleaner_for_bp.percent_cleaner, 
	avg(distinct bbb) as bp_by_bp,
	component_id, 
	post_id, 
	round(avg(temperature)) temperature, 
	test_table.anchor::text, 
	surname, name, lastname 
from (select *, avg(rank) over(partition by anchor) avg_rank, abs(start_time::bigint - lag(end_time::bigint) over(partition by email, component_name, anchor order by id)) as bbb from table_main) as test_table 
left join (select email, count(distinct id) count_bypass, sum(DISTINCT time_bypasses::bigint) as time_bypasses, anchor from table_main group by anchor, email) as t_v2 on t_v2.anchor = test_table.anchor 
left join (select anchor, sum(cleaner), round((sum(cleaner)::float / count(distinct id)::float)::float * 100)::text || '%%' as percent_cleaner from (select id, cleaner, anchor from table_main group by anchor, id, email, cleaner) as c_cleaner group by anchor) as is_cleaner_for_bp on test_table.anchor = is_cleaner_for_bp.anchor 
group by test_table, test_table.anchor, test_table.email, avg_rank, component_name, post_name, component_id, post_id, surname, name, lastname, t_v2.count_bypass, t_v2.time_bypasses, is_cleaner_for_bp.percent_cleaner), 
result_final as ( 
select email, post_name, component_name, round(avg(avg_component) over(partition by component_id, anchor), 1), avg_rank, time_bypasses, count_bypass, percent_cleaner, sum(bp_by_bp) over(partition by anchor)/count(*) over(partition by anchor)  as bp_by_bp , component_id, post_id, temperature, anchor, surname, name, lastname, bp_by_bp from users_detail_month_week) 
select * from result_final order by anchor, component_id asc;
"""

USERS_LIST_QUERY_AVG_YEAR = """
with table_main as (select distinct p.id post_id, u.surname, u.name, 
    u.lastname, b.id, building.name building_name, p.name post_name, c.name component_name, 
    AVG(cr.rank::decimal) rank, SUM(DISTINCT b.end_time::bigint - b.start_time::bigint) time_bypasses, 
    COUNT(DISTINCT  b.id) count_bypass, u.email as email, cleaner, b.start_time, b.end_time, cr.component_id, substring(to_timestamp(to_timestamp(((3 * 60 * 60 * 1000) + b.end_time::bigint) / 1000.0)::date::text, 'YYYY-MM')::text from 3 for 5) as anchor, to_timestamp(((3 * 60 * 60 * 1000) + b.end_time::bigint) / 1000.0)::date as anchor_day, avg(b.temperature) temperature, max(us.create_date) over(partition by b.id) as create_date from building left join post p
    on building.id = p.building_id left join bypass b on p.id = b.post_id 
    left join bypass_rank br on b.id = br.bypass_id left join component_rank 
    cr on br.component_rank_id = cr.id left join component c 
    on br.component_id = c.id left join public.user u on b.user_id = u.id 
    left join user_shift us on us.create_date::bigint < b.start_time::bigint
    WHERE b.finished=1 and p.name = {0} and email = {1} and b.end_time::bigint > {2} and b.end_time::bigint <= {3}
    GROUP BY u.id, p.id, c.id, b.id, building_name, cr.component_id, us.create_date),
users_detail_month_week as ( 
select 
	test_table.email, 
	post_name, 
	component_name, 
	round(avg(rank), 1) avg_component, 
	round(avg_rank, 1) avg_rank, 
	t_v2.time_bypasses, 
	t_v2.count_bypass, 
	is_cleaner_for_bp.percent_cleaner, 
	avg(distinct bbb)::int as bp_by_bp,
	component_id, 
	post_id, 
	round(avg(temperature)) temperature, 
	test_table.anchor::text, 
	surname, name, lastname 
from (select *, avg(rank) over(partition by anchor) avg_rank, abs(start_time::bigint - lag(end_time::bigint) over(partition by email, component_name, anchor_day order by id)) as bbb from table_main) as test_table 
left join (select email, count(distinct id) count_bypass, sum(DISTINCT time_bypasses::bigint) as time_bypasses, anchor from table_main group by anchor, email) as t_v2 on t_v2.anchor = test_table.anchor 
left join (select anchor, sum(cleaner), round((sum(cleaner)::float / count(distinct id)::float)::float * 100)::text || '%%' as percent_cleaner from (select id, cleaner, anchor from table_main group by anchor, id, email, cleaner) as c_cleaner group by anchor) as is_cleaner_for_bp on test_table.anchor = is_cleaner_for_bp.anchor 
group by test_table, test_table.anchor, test_table.email, avg_rank, component_name, post_name, component_id, post_id, surname, name, lastname, t_v2.count_bypass, t_v2.time_bypasses, is_cleaner_for_bp.percent_cleaner), 
result_final as ( 
select email, post_name, component_name, round(avg(avg_component) over(partition by component_id, anchor), 1), avg_rank, time_bypasses, count_bypass, percent_cleaner, sum(bp_by_bp) over(partition by anchor)/count(*) over(partition by anchor)  as bp_by_bp , component_id, post_id, temperature, anchor, surname, name, lastname, bp_by_bp from users_detail_month_week) 
select * from result_final order by anchor, component_id asc;
"""

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

USERS_DETAIL_LIST_VIEW = "select bypass.id bypass_id, bypass.user_id user_id, bypass.start_time bypass_start_time, bypass.end_time bypass_end_time, bypass.weather bypass_weather, bypass.temperature bypass_temperature, bypass.cleaner bypass_cleaner, br.id bypass_rank_id, br.component_id bypass_rank_component_id, c.name component_name, c.description component_description, br.component_rank_id bypass_rank_component_rank_id, cr.rank component_rank_rank, cr.name component_rank_name, u.email user_email, p.name post_name, bypass.icon bypass_icon, u.surname user_surname, u.name user_name, u.lastname user_lastaname, br.is_image " \
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
 	"where b.name = {1} and bypass.finished = 1 and bypass.end_time::bigint > {0} and bypass.end_time::bigint <= {2};"

QUERY_GET_OBJECTS = '''
with temp_req as (select building.id as building_id_main, building.name as build_name, p.name as post_name, AVG(cr.rank::decimal) rank, 
        SUM(DISTINCT b.end_time::bigint - b.start_time::bigint) time_bypasses, 
        COUNT(DISTINCT  b.id) count_bypass, building.id as building_id from building 
        left join post p on building.id = p.building_id 
        left join bypass b on p.id = b.post_id 
        left join bypass_rank br on b.id = br.bypass_id 
        left join component_rank cr 
        on br.component_rank_id = cr.id 
        left join component c on br.component_id = c.id 
        left join public.user u on u.id = b.user_id 
        WHERE b.finished=1 and b.end_time::bigint >= {0} and b.end_time::bigint <= {1}
        GROUP BY p.id, build_name, building.id),
cycle_for_buildings as (select building_id, count(*) as cnt 
	from user_cycle_of_bypass ucob2 
	where end_time::bigint >= {0} and end_time::bigint <= {1}
	group by building_id),
	cycle_table as (
select 
ucob.user_id, 
building_id, end_time, 
create_date_row, 
max(create_date::bigint)::bigint as create_date
from user_cycle_of_bypass ucob 
left join user_shift us on us.user_id = ucob.user_id and ucob.end_time > us.create_date 
where end_time::bigint >= {0} and end_time::bigint <= {1}
group by ucob.user_id, building_id, end_time, create_date_row order by create_date desc),
cycle_table2 as (
select cycle_table.user_id, building_id, end_time, create_date_row, cycle_table.create_date, cycle_table.create_date_row + to_timestamp(start_shift::bigint /1000.0 + 10800)::time start_shift from cycle_table left join user_shift us on us.create_date::bigint = cycle_table.create_date::bigint order by end_time),
cycle_table_bbb as (
select *, abs(end_time::bigint - lag(end_time::bigint) over(partition by user_id, building_id, create_date_row)) as bbb from cycle_table2),
cycle_table_pre_res as (
select building_id, case when bbb is null then abs(extract(epoch from start_shift) * 1000 - end_time::bigint - 10800000) else bbb end as bbb from cycle_table_bbb),
cycle_table_final as (
select building_id, avg(bbb)::bigint as time_between_bypass from cycle_table_pre_res group by building_id),
pre_res as (
select b1.building_id, build_name, (select b5.post_name 
from temp_req b5 
where b5.rank = 
(select max(rank) from temp_req b5 where b1.build_name = b5.build_name) and b1.build_name = b5.build_name limit 1) as 
            best_post, (select max(rank) from temp_req tv where b1.build_name = tv.build_name limit 1) best_rank,
            (select b2.post_name 
            from temp_req b2 where b2.rank = 
            (select min(rank) from temp_req b2 where b1.build_name = b2.build_name) and b1.build_name = b2.build_name limit 1) 
            as bad_post, (select min(rank) 
            from temp_req b2 where b1.build_name = b2.build_name limit 1) bad_rank, 
            SUM(time_bypasses::integer) times, SUM(count_bypass::integer) 
            count, AVG(rank) avg_rank, coalesce(cnt, 0) as "cycle" from temp_req b1 left join cycle_for_buildings as cfb on cfb.building_id = b1.building_id group by build_name, b1.building_id, cnt)
select pre_res.building_id, build_name, best_post, best_rank, bad_post, bad_rank, times, count, avg_rank, "cycle", coalesce(time_between_bypass, 0) as time_between_bypass from pre_res left join cycle_table_final on cycle_table_final.building_id = pre_res.building_id;
'''

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
            "where post.name is not null and bypass.end_time::bigint > {0} " \
            "and post.name = b1.post_name and bypass.finished=1) count_users " \
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

QUERY_GET_USERS_DETAIL = 'select  bypass_id, user_id, bypass_start_time, bypass_end_time, bypass_weather, bypass_temperature, bypass_cleaner, bypass_rank_id, bypass_rank_component_id, component_name, component_description, bypass_rank_component_rank_id, component_rank_rank, component_rank_name, user_email, post_name, bypass_icon, user_surname, user_name, user_lastaname, is_image ' \
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

QUERY_STAT_SINGLE_PERSON = "select u.id, round(avg(cr.rank::numeric), 2), " \
                          "count(distinct b.id), min(cnt_bp.cnt) from " \
                          "(select \"user\".id user_id, post.id post_id, " \
                          "count(b.id) as cnt from post left join " \
                          "(bypass b inner join \"user\" on \"user\".id " \
                          "= b.user_id) on b.post_id = post.id " \
                          "and b.end_time::bigint >= {1} " \
                          "and \"user\".id = {0} " \
                          "GROUP by post.id, \"user\".id) as cnt_bp, " \
                          "bypass_rank br left join bypass b on b.id " \
                          "= br.bypass_id and b.finished = 1 " \
                          "and b.end_time::bigint >= {1} " \
                          "left join \"user\" u  on b.user_id = u.id " \
                          "left join component_rank cr on cr.id " \
                          "= br.component_rank_id " \
                          "where u.id = {0} group by u.id;"

QUERY_GET_BASE_STATIC_BY_USER = '''
with 
	cnt_by_bypasses as (
		SELECT 
			"user".id AS user_id, 
			post.id AS post_id, 
			COUNT(*) AS cnt 
		FROM 
			post 
		LEFT JOIN 
			(bypass b 
				inner JOIN 
					"user" 
				ON 
					"user".id = b.user_id
			) 
		ON 
			b.post_id = post.id 
		AND 
			b.end_time::BIGINT >= {0}
		and b.end_time::BIGINT <= {1}
		where b.finished = 1
		GROUP BY 
			post.id, "user".id
	order by user_id
	),
	user_and_bypass as (
		select "user".id user_id, post.id post_id
		from "user", post 
		order by "user".id
	),
	pre_result as(
		select 
			u.id, 
			round(avg(cr.rank::numeric), 1) AS avg_rank, 
			COUNT(DISTINCT b.id) count_bypass, 
			MIN(cnt_bp.cnt) as "cycle", 
			SUM(distinct b.end_time::BIGINT - b.start_time::BIGINT) AS time_bypass
		FROM 
			(select uab.user_id, uab.post_id, coalesce(cnt, 0) as cnt from user_and_bypass uab full join cnt_by_bypasses cbb on uab.user_id = cbb.user_id and uab.post_id = cbb.post_id) AS cnt_bp, 
				bypass_rank br
		LEFT JOIN 
			bypass b 
		ON 
			b.id = br.bypass_id 
		AND 
			b.finished = 1 
		AND 
			b.end_time::BIGINT >= {0}
		and b.end_time::BIGINT <= {1}
		LEFT JOIN 
			"user" u 
		ON 
			b.user_id = u.id 
		LEFT JOIN 
			component_rank cr 
		ON 
			cr.id = br.component_rank_id 
		where cnt_bp.user_id = u.id 
		GROUP BY 
			u.id
		),
		get_all_bp as (
	select
		distinct on(b.id)
		b.id,
		b.user_id, 
		post_id, 
		start_time::bigint, 
		end_time::bigint, 
		max(us.create_date) over(partition by b.id) as create_date,
		to_timestamp(end_time::bigint / 1000.0)::date as anchor,
		us.start_shift
	from 
		bypass b, 
		user_shift us
	where 
		b.user_id = us.user_id 
	and 
		us.create_date::bigint < b.start_time::bigint
	
	and
	b.end_time::BIGINT >= {0}
		and b.end_time::BIGINT <= {1}
		and
		b.finished = 1
	group by b.id, us.start_shift, us.create_date 
),
get_result_with_bbb as (
	select *, abs(start_time - lag(end_time) over(partition by user_id, anchor order by end_time)) as bbb from get_all_bp
),
	--select '9 hour'
	--select to_char(to_timestamp(start_time)::date  + interval time_i:char, 'DD-MM-YY HH24:MI:SS'), time_i from get_result_with_bbb
get_create_date_calc as (select 
	get_result_with_bbb.id, 
	get_result_with_bbb.user_id, 
	post_id, 
	start_time, 
	end_time, 
	get_result_with_bbb.create_date, 
	anchor, 
	case when bbb is null then 0 else bbb end,
	anchor + to_timestamp(us.start_shift::bigint /1000.0)::time start_shift 
	from get_result_with_bbb 
	left join user_shift us on us.create_date = get_result_with_bbb.create_date
	),
final_table as (
	select *, case when bbb = 0 then abs((extract(epoch from start_shift) * 1000 - start_time)) else bbb end as result_bbb from get_create_date_calc as gcdc
)

select pre_result.id, avg_rank, count_bypass, "cycle", time_bypass, round(avg(result_bbb)) as tbr from pre_result left join final_table ft on ft.user_id = pre_result.id group by pre_result.id, pre_result.avg_rank, pre_result.count_bypass, pre_result."cycle", pre_result.time_bypass
'''
QUERY_GET_STATIC_BY_USER_WITH_TBR = '''
with 
	cnt_by_bypasses as (
		select user_id, building_id, count(*) as cnt 
		from user_cycle_of_bypass ucob2 
		where end_time::bigint >= {0} and end_time::bigint <= {1} and building_id = {2}
		group by user_id, building_id
	),
	user_and_bypass as (
		select "user".id user_id, post.id post_id
		from "user", post 
		where post.building_id = {2}
		order by "user".id
	),
	pre_result as(
		select 
			u.id, 
			round(avg(cr.rank::numeric), 1) AS avg_rank, 
			COUNT(DISTINCT b.id) count_bypass, 
			MIN(cnt_bp.cnt) as "cycle", 
			SUM(distinct b.end_time::BIGINT - b.start_time::BIGINT) AS time_bypass,
			u.surname as surname, u.name as first_name, u.lastname,
			p.building_id 
		FROM 
			(select uab.user_id, uab.post_id, coalesce(cnt, 0) as cnt from user_and_bypass uab full join cnt_by_bypasses cbb on uab.user_id = cbb.user_id) AS cnt_bp, 
				bypass_rank br
		LEFT JOIN 
			bypass b 
		ON 
			b.id = br.bypass_id 
		left join post p 
		on p.id = b.post_id 
		AND 
			b.finished = 1 
		AND 
			b.end_time::BIGINT >= {0}
		and b.end_time::BIGINT <= {1}
		LEFT JOIN 
			"user" u 
		ON 
			b.user_id = u.id 
		LEFT JOIN 
			component_rank cr 
		ON 
			cr.id = br.component_rank_id 
		where cnt_bp.user_id = u.id and p.building_id = {2}
		GROUP BY 
			u.id, u.surname, u.name, u.lastname, p.building_id 
		),
		get_all_bp as (
	select
		distinct on(b.id)
		b.id,
		b.user_id, 
		post_id, 
		start_time::bigint, 
		end_time::bigint, 
		max(us.create_date) over(partition by b.id) as create_date,
		to_timestamp(end_time::bigint / 1000.0)::date as anchor,
		us.start_shift
	from 
		bypass b, 
		user_shift us
	where 
		b.user_id = us.user_id
	and 
		us.create_date::bigint < b.start_time::bigint
	and
	b.end_time::BIGINT >= {0}
		and b.end_time::BIGINT <= {1}
		and
		b.finished = 1
	group by b.id, us.start_shift, us.create_date 
),
get_result_with_bbb as (
	select pre_result.id, avg_rank, count_bypass, "cycle", time_bypass, ucob.user_id, ucob.building_id, ucob.end_time, abs(ucob.end_time::bigint - lag(ucob.end_time::bigint) over(partition by ucob.user_id, create_date_row)) as bbb, 
	 create_date_row, get_all_bp.create_date, min(get_all_bp.start_time) start_time from pre_result 
left join user_cycle_of_bypass ucob on ucob.user_id = pre_result.id and ucob.end_time::BIGINT >= {0} and ucob.end_time::BIGINT <= {1} and ucob.building_id = pre_result.building_id
join get_all_bp on pre_result.id = get_all_bp.user_id group by pre_result.id, avg_rank, count_bypass, "cycle", time_bypass, ucob.user_id, ucob.building_id, ucob.end_time, create_date_row, get_all_bp.create_date
),
get_create_date_calc as (select 
	get_result_with_bbb.id,  
	building_id, 
	start_time,
	end_time, 
	get_result_with_bbb.create_date_row, 
	case when bbb is null then 0 else bbb end,
	get_result_with_bbb.create_date_row + to_timestamp(us.start_shift::bigint /1000.0  + 10800)::time start_shift 
	from get_result_with_bbb 
	left join user_shift us on us.create_date = get_result_with_bbb.create_date
	),
final_table as (
	select *, case when bbb = 0 then abs((extract(epoch from start_shift) * 1000 - end_time::bigint - 10800000)) else bbb end as result_bbb from get_create_date_calc as gcdc
)
select pre_result.id, avg_rank, count_bypass, coalesce("cycle", 0) as "cycle", time_bypass, pre_result.building_id, case when round(avg(result_bbb)) is null then 0 else round(avg(result_bbb)) end as tbr, pre_result.surname, pre_result.first_name, pre_result.lastname from pre_result
left join final_table ft on ft.id = pre_result.id
group by pre_result.id, pre_result.avg_rank, pre_result.count_bypass, pre_result."cycle", pre_result.time_bypass, pre_result.building_id, pre_result.surname, pre_result.first_name, pre_result.lastname;
'''

QUERY_GET_STATIC_BY_USER_WITH_TBR_DETAIL = '''
with user_bypass as (
select distinct u.id as user_id, building.id as building_id, p.id post_id, b.id as bypass_id, c.id as component_id, cr.id as component_rank_id, br.id as bypass_rank_id, u.surname, u.name as user_name, 
    u.lastname, u.email, p.name as post_name, c.name as component_name, 
    cr.rank::decimal rank_component, SUM(DISTINCT b.end_time::bigint - b.start_time::bigint) time_bypasses, 
    COUNT(DISTINCT  b.id) count_bypass, cleaner, b.start_time, b.end_time, to_timestamp(((3 * 60 * 60 * 1000) + b.end_time::bigint) / 1000.0)::date as anchor, b.temperature temperature, max(us.create_date) over(partition by b.id) as create_date, avg(cr."rank"::float) over(partition by p.id) as avg_rank_post from building left join post p
    on building.id = p.building_id left join bypass b on p.id = b.post_id 
    left join bypass_rank br on b.id = br.bypass_id left join component_rank 
    cr on br.component_rank_id = cr.id left join component c 
    on br.component_id = c.id left join public.user u on b.user_id = u.id 
    left join user_shift us on us.create_date::bigint < b.start_time::bigint
    WHERE b.finished=1 and u.id = {0} and building.id = {1} and b.end_time::bigint >= {2} and b.end_time::bigint <= {3}
    GROUP BY u.id, p.id, c.id, b.id, building.id, cr.component_id, br.id, cr.id, us.create_date),
table_main as (
	select 
	user_bypass.user_id, 
	building_id, 
	post_id, 
	bypass_id, 
	component_id, 
	component_rank_id, 
	bypass_rank_id, 
	surname, 
	user_name, 
	lastname, 
	email, 
	post_name, 
	component_name, 
	rank_component, 
	time_bypasses, 
	count_bypass, 
	cleaner, 
	start_time, 
	end_time, 
	temperature, 
	user_bypass.create_date, 
	anchor as start_shift,
	avg_rank_post
	from user_bypass 
	left join user_shift us on us.create_date = user_bypass.create_date),
table_help as (select c.id as component_id, cr.id as component_rank_id_help, "rank" all_rank from component as c left join component_rank as cr on c.id = cr.component_id),
pre_result as (select tul.email, tul.bypass_id, tul.component_id, min(all_rank) as "rank" from table_main as tul left join table_help th on th.component_id = tul.component_id
		and tul.bypass_id > component_rank_id_help 
		group by tul.bypass_id, tul.component_id, email),
	clear_best_single_rank as (select * from pre_result where "rank"::float != 5.0),
is_min_rank as (
	select user_id, 
	building_id, 
	post_id, 
	table_main.bypass_id, 
	table_main.component_id, 
	component_rank_id, 
	bypass_rank_id, 
	surname, 
	user_name, 
	lastname, 
	table_main.email, 
	post_name, 
	component_name, 
	rank_component, 
	time_bypasses, 
	count_bypass, 
	cleaner, 
	start_time, 
	end_time, 
	temperature, 
	create_date, 
	start_shift,
	case when cbsr."rank"::float = table_main.rank_component::float then 1 else 0 end as is_minimal_rank, avg_rank_post from table_main left join clear_best_single_rank cbsr on table_main.bypass_id = cbsr.bypass_id and table_main.component_id = cbsr.component_id order by bypass_id asc
),
table_for_compressed_bypass as (
	select post_id, bypass_id, start_time, end_time, count(*), start_shift from table_main group by post_id, bypass_id, start_time, end_time, start_shift order by post_id
),
table_for_bypass_by_bypass as (
	select *, abs(start_time::bigint - lag(end_time::bigint) over(partition by post_id, start_shift)) as avg_bp_by_bp from table_for_compressed_bypass
),
table_for_bypass_by_bypass_two as (
	select post_id, bypass_id, start_time, end_time, start_shift, (sum(avg_bp_by_bp) over(partition by post_id)/count(*) over(partition by post_id))::bigint  as avg_bp_by_bp from table_for_bypass_by_bypass
),
table_for_bypass_by_bypass_group as (
	select post_id, avg_bp_by_bp from table_for_bypass_by_bypass_two group by post_id, avg_bp_by_bp
),
compressed_row as (
select user_id, building_id,
	is_min_rank.post_id, 
	is_min_rank.component_id, 
	surname, 
	user_name, 
	lastname,
	email, 
	post_name, 
	component_name, 
	round(avg_rank_post::decimal, 1)::float as avg_rank_post,
	round(avg(rank_component), 1) avg_rank_component,
	sum(count_bypass) as count_bypass,
	round((sum(cleaner)::numeric / count(is_min_rank.bypass_id)::numeric) * 100) || '%%' as percent_cleaner, 
	sum(distinct time_bypasses) as time_bypasses,
	case when avg_bp_by_bp is null then 0 else avg_bp_by_bp end as avg_bp_by_bp,
	round(avg(temperature)) as avg_temperature,
	sum(is_min_rank.is_minimal_rank) as count_bad_rank_component from is_min_rank left join table_for_bypass_by_bypass_group as tfbg on tfbg.post_id = is_min_rank.post_id  group by user_id, is_min_rank.post_id, is_min_rank.component_id, surname, 
	user_name, 
	lastname, 
	email, 
	post_name, 
	component_name, avg_rank_post, building_id, avg_bp_by_bp)
select * from compressed_row;
'''

QUERY_GET_PHOTO_FOR_USER_OF_POST = '''with pre_res as (select b.id as bypass_id, b.user_id, 
        b.post_id,  
        b.weather, 
        b.temperature, 
        b.icon, 
        br.id as bypass_rank_id, 
        br.component_id, 
        br.component_rank_id, 
        prg.image,
        u.email,
        u.surname,
        u."name",
        u.lastname,
       	cr."rank" fact_rank,
       	cr.id as component_rank_id_actual,
       	prg.id as time_ms_make_photo,
       	min(cr."rank") over(partition by br.component_rank_id) min_rank
        from bypass b left join bypass_rank br on br.bypass_id = b.id left join "user" u on u.id = b.user_id left join photo_rank_gallery prg on prg.bypass_rank_id = br.id left join component_rank cr on cr.component_id = br.component_id
        where is_image = true and bypass_id >  cr.id and  br.component_id = {0} and b.post_id = {1} and u.email = {2} and b.end_time::bigint >= {3} and b.end_time::bigint <= {4}),
res as (select * from pre_res where fact_rank = min_rank)
select * from res where component_rank_id = component_rank_id_actual and min_rank != '5.0' order by time_ms_make_photo desc limit {5} offset {6};'''

QUERY_GET_COMPONENT_FOR_BUILDING = """
with component_list as (
select 
	bypass.id as bypass_id, 
	br.id as bypass_rank_id, 
	bul.id as building_id, 
	c.id as component_id, 
	c."name" as component_name, 
	cr."name" as component_rank_name, 
	cr."rank" as component_rank_rank, 
	br.start_time, br.end_time,
	avg(cr."rank"::float) over() avg_rank
	from bypass 
		left join bypass_rank br  on br.bypass_id = bypass.id  
		left join component c  on c.id = br.component_id 
		left join component_rank cr  on cr.component_id = c.id and br.component_rank_id = cr.id
		left join "user" u  on u.id = bypass.user_id
		left join post p on p.id = bypass.post_id 
		left join building bul on bul.id = p.building_id 
	where bypass.finished = 1 and bypass.end_time::bigint >= {0} and bypass.end_time::bigint <= {1} and bul.id = {2}),
compressed_component_list as (
select building_id, component_id, component_name, round(avg(component_rank_rank::numeric), 1) as avg_rank_component, count(bypass_rank_id) count_bypass_rank, sum(distinct end_time::bigint - start_time::bigint) as time_bypasses_component, round(avg_rank::numeric, 1) avg_rank from component_list group by building_id, component_id, component_name, avg_rank)
select *, sum(time_bypasses_component) over() sum_time_bypasses_component, sum(count_bypass_rank) over () as sum_qr_scan_component from compressed_component_list ;
"""

from time import time
STATIC_DAY = int(time() * 1000) % 86400000
TODAY_MILLISECONDS = 0
WEEK_MILLISECONDS = 518400000
MONTH_MILLISECONDS = 2678400000
YEAR_MILLISECONDS = 31536000000
TAIL_TODAY = 86400000

if __name__ == '__main__':
    print(USERS_LIST_QUERY_AVG.format(1625946644430))
    print(USERS_LIST_VIEW % (1629752400000, 1629752400000 + 86400000))