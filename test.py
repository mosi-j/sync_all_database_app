from sync_database import SyncSiteAppDataBase, SyncBackTestAppDataBase
from my_time import get_now_time_second, get_now_time_string
from time import sleep


source_db_info = {'db_name': 'new_bourse_client_0.1',
                  'db_username': 'bourse_user',
                  'db_user_password': 'Asdf1234',
                  'db_host_name': 'localhost',
                  'db_port': 3306}

destination_db_info = {'db_name': 'asd',
                       'db_username': 'bourse_user',
                       'db_user_password': 'Asdf1234',
                       'db_host_name': 'localhost',
                       'db_port': 3306}

transaction_mod = True
max_packet_size = 1000000
sync_period_time = 1000000
cycle_period_time = 60 * 1

obj1 = SyncSiteAppDataBase(source_db_info=source_db_info, destination_db_info=destination_db_info,
                           max_packet_size=max_packet_size, transaction_mod=transaction_mod,
                           sync_period_time=sync_period_time)

obj2 = SyncBackTestAppDataBase(source_db_info=source_db_info, destination_db_info=destination_db_info,
                               max_packet_size=max_packet_size, transaction_mod=transaction_mod,
                               sync_period_time=sync_period_time)

while True:
    start_cycle_time = get_now_time_second()

    obj1.run(autocommit_each_table=True)
    obj2.run(autocommit_each_table=True)

    sleep_time = start_cycle_time + cycle_period_time - get_now_time_second()

    if sleep_time > 0:
        print('sleep sync select_process. start sleep: {0} sleep_time: {1}'.format(get_now_time_string(), sleep_time))
        sleep(sleep_time)
