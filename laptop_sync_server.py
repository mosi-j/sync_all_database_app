
from sync_database import SyncDataBase
from my_time import get_now_time_second, get_now_time_string, get_now_time_datetime
from time import sleep

from my_database_info import get_database_info, laptop_local_access, server_lan_access
from my_database_info import website_data, tsetmc_and_analyze_data

from datetime import timedelta

sync_max_last_day = 5
max_packet_size = 4000000
commit_mod = 'each_pocket'
sync_period_time = 60 * 10
cycle_period_time = 60 * 20


# bourse_tsetmc_raw_data to bourse_analyze_data(index_data, index_info, open_days, shareholders_data, share_adjusted_data, share_daily_data, share_info, share_second_data, share_status)
# bourse_analyze_data to bourse_website_data (open_days, share_info)

# ------------------------------------------
# vps1: bourse_tsetmc_and_analyze_data to vps1: bourse_website_data (open_days, share_info)
from my_database_info import open_days, share_info

obj_1_rule_table = [[open_days, open_days, None],
                    [share_info, share_info, None]]

obj_1 = SyncDataBase(source_db_info=get_database_info(pc_name=server_lan_access, database_name=tsetmc_and_analyze_data),
                   destination_db_info=get_database_info(pc_name=server_lan_access, database_name=website_data),
                   max_packet_size=max_packet_size,
                   commit_mod=commit_mod,
                   sync_period_time=sync_period_time,
                   rule_table=obj_1_rule_table)

# vps1: bourse_tsetmc_and_analyze_data to server: bourse_tsetmc_and_analyze_data
# (excel_share_daily_data, index_data, index_info, open_days, shareholders_data, share_adjusted_data, share_daily_data, share_info, share_second_data, share_status, share_sub_trad_data)
from my_database_info import excel_share_daily_data, index_data, index_info, shareholders_data, share_adjusted_data, share_daily_data, share_second_data, share_status, share_sub_trad_data

obj_2_rule_table = [[excel_share_daily_data, excel_share_daily_data, None],
                    [index_data, index_data, None],
                    [index_info, index_info, None],
                    [open_days, open_days, None],
                    [shareholders_data, shareholders_data, None],
                    [share_adjusted_data, share_adjusted_data, None],
                    [share_daily_data, share_daily_data, None],
                    [share_info, share_info, None],
                    [share_second_data, share_second_data, None],
                    [share_status, share_status, None],
                    [share_sub_trad_data, share_sub_trad_data, None],
                    ]



obj_2 = SyncDataBase(source_db_info=get_database_info(pc_name=server_lan_access, database_name=tsetmc_and_analyze_data),
                     destination_db_info=get_database_info(pc_name=laptop_local_access, database_name=tsetmc_and_analyze_data),
                     max_packet_size=max_packet_size,
                     commit_mod=commit_mod,
                     sync_period_time=sync_period_time,
                     rule_table=obj_2_rule_table,
                     clean_sync=False)


# vps1: bourse_website_data to vps1: bourse_tsetmc_and_analyze_data (strategy, strategy) clean
from my_database_info import strategy, back_test_result
obj_3_rule_table = [[strategy, strategy, None],
                    [back_test_result, back_test_result, None]]

obj_3 = SyncDataBase(source_db_info=get_database_info(pc_name=server_lan_access, database_name=tsetmc_and_analyze_data),
                     destination_db_info=get_database_info(pc_name=server_lan_access, database_name=tsetmc_and_analyze_data),
                     max_packet_size=max_packet_size,
                     commit_mod=commit_mod,
                     sync_period_time=sync_period_time,
                     rule_table=obj_3_rule_table,
                     clean_sync=True)


first_loop = True
while True:
    if first_loop is True:
        force = True
        first_loop = False
    else:
        force = False

        a = get_now_time_datetime() - timedelta(days=sync_max_last_day)
        where_start_date_m = a.year * 10000 + a.month * 100 + a.day
        print(where_start_date_m)

        obj_2_rule_table_where = [[excel_share_daily_data, excel_share_daily_data, None],
                                  [index_data, index_data, "date_m > {}".format(where_start_date_m)],
                                  [index_info, index_info, None],
                                  [open_days, open_days, None],
                                  [shareholders_data, shareholders_data, "date_m > {}".format(where_start_date_m)],
                                  [share_adjusted_data, share_adjusted_data, "date_m > {}".format(where_start_date_m)],
                                  [share_daily_data, share_daily_data, "date_m > {}".format(where_start_date_m)],
                                  [share_info, share_info, None],
                                  [share_second_data, share_second_data, "date_time > {}".format(where_start_date_m * 1000000)],
                                  [share_status, share_status, None],
                                  [share_sub_trad_data, share_sub_trad_data, "date_m > {}".format(where_start_date_m)],
                                  ]
        obj_2.set_rull_table(obj_2_rule_table_where)

    start_cycle_time = get_now_time_second()

    #obj_1.select_query(force=force)
    obj_2.run(force=force)
    #obj_3.select_query(force=True)

    cycle_runtime =  get_now_time_second() - start_cycle_time
    print('cycle_runtime: {}'.format(cycle_runtime))

    sleep_time = start_cycle_time + cycle_period_time - get_now_time_second()

    if sleep_time > 0:
        print('sleep sync select_process. start sleep: {0} sleep_time: {1}'.format(get_now_time_string(), sleep_time))
        sleep(sleep_time)
