# database info
tsetmc_raw_data = 'bourse_tsetmc_raw_data'
analyze_data = 'bourse_analyze_data'
website_data = 'bourse_website_data'
tsetmc_and_analyze_data = 'bourse_tsetmc_and_analyze_data'




# database info ----------
open_days = {'name': 'open_days', 'pk_field': 'date_m'}
index_data = {'name': 'index_data', 'pk_field': 'en_index_12_digit_code, date_m'}
user = {'name': 'user', 'pk_field': 'username'}
strategy = {'name': 'strategy', 'pk_field': 'user_name, strategy_name'}
share_status = {'name': 'share_status', 'pk_field': 'en_symbol_12_digit_code, date_m, change_time, change_number'}
share_second_data = {'name': 'share_second_data', 'pk_field': 'en_symbol_12_digit_code, date_time'}
share_info = {'name': 'share_info', 'pk_field': 'en_symbol_12_digit_code'}
share_daily_data = {'name': 'share_daily_data', 'pk_field': 'en_symbol_12_digit_code, date_m'}
share_adjusted_data = {'name': 'share_adjusted_data', 'pk_field': 'en_symbol_12_digit_code, date_m, adjusted_type'}
shareholders_data = {'name': 'shareholders_data', 'pk_field': 'en_symbol_12_digit_code, date_m, sh_id'}
index_info = {'name': 'index_info', 'pk_field': 'en_index_12_digit_code'}
back_test_result = {'name': 'back_test_result', 'pk_field': 'order_id'}
client_local_settings = {'name': 'client_local_settings', 'pk_field': 'client_id'}
excel_share_daily_data = {'name': 'excel_share_daily_data', 'pk_field': 'en_symbol_12_digit_code, date_m'}
fail_hang_share = {'name': 'fail_hang_share', 'pk_field': 'en_symbol_12_digit_code, date_m'}
fail_integrity_share = {'name': 'fail_integrity_share', 'pk_field': 'en_symbol_12_digit_code, date_m'}
fail_other_share = {'name': 'fail_other_share', 'pk_field': 'en_symbol_12_digit_code, date_m'}
share_adjusted_daily_data = {'name': 'share_adjusted_daily_data', 'pk_field': 'en_symbol_12_digit_code, date_m'}
share_sub_trad_data = {'name': 'share_sub_trad_data', 'pk_field': 'en_symbol_12_digit_code, date_m, trad_number'}
db_setting = {'name': 'db_setting', 'pk_field': 'update_time'}  # need change
order_result = {'name': 'order_result', 'pk_field': 'order_id'}
sub_order_result = {'name': 'sub_order_result', 'pk_field': 'order_id, symbol'}
waiting_order = {'name': 'waiting_order', 'pk_field': 'order_id'}

# global ------------------------
remote_port = 3306

pc_info = {'local_hostname': 'localhost',
           'local_port': 3306,

            # vps1 ------------------------
            'vps1_remote_username': 'tsetmc_raw_data_',
            'vps1_remote_password': '9!b$#4Kx7W5g2L#pvmEta7Afc#6PgYz7mmaexr@2@!zR4hgt/NM!nb$zAP-Y&eULz6KqW%^EUZAd6XsywpqfYgzS^RXmnv_v@4JF76xeU=z3KSe7',
            'vps1_remote_hostname': '178.63.149.85',
            'vps1_remote_port': remote_port,
            # ----------------------
            'vps1_local_username': 'tsetmc_raw_data',
            'vps1_local_password': '9!b$#4Kx7W5g2L#p',

            # server ------------------------
            'server_remote_username_vps': 'bours_R_vps',
            'server_remote_password_vps': '!mVVx2*Mr3WRNyVh19Uxc%pAk',
            'server_remote_hostname': '2.184.236.202',
            'server_remote_port': remote_port,
            # ----------------------
            'server_local_username': 'bourse_user',
            'server_local_password': 'Asdf1234',

            # ----------------------
            'server_lan_username': 'bourse_user_g',
            'server_lan_password': 'Asdf1234',
            'server_lan_hostname': '192.168.1.5',
            'server_lan_port': remote_port,

            # server ------------------------
            'laptop_local_username': 'bourse_user',
            'laptop_local_password': 'Asdf1234',
           }

vps1_local_access = 'vps1_local_access'
vps1_remote_access = 'vps1_remote_access'
server_local_access = 'server_local_access'
server_remote_access_from_vps = 'server_remote_access_from_vps'
server_lan_access = 'server_lan_access'
laptop_local_access = 'laptop_local_access'


def get_database_info(pc_name, database_name):
    res = None
    # local ----------------------
    if pc_name == 'vps1_local_access':
        res = {'db_name': database_name,
               'db_username': pc_info['vps1_local_username'],
               'db_user_password': pc_info['vps1_local_password'],
               'db_host_name': pc_info['local_hostname'],
               'db_port': pc_info['local_port']}

    elif pc_name == 'server_local_access':
        res = {'db_name': database_name,
               'db_username': pc_info['server_local_username'],
               'db_user_password': pc_info['server_local_password'],
               'db_host_name': pc_info['local_hostname'],
               'db_port': pc_info['local_port']}

    elif pc_name == 'laptop_local_access':
        res = {'db_name': database_name,
               'db_username': pc_info['laptop_local_username'],
               'db_user_password': pc_info['laptop_local_password'],
               'db_host_name': pc_info['local_hostname'],
               'db_port': pc_info['local_port']}
    # remote ----------------------
    elif pc_name == 'vps1_remote_access':
        res = {'db_name': database_name,
               'db_username': pc_info['vps1_remote_username'],
               'db_user_password': pc_info['vps1_remote_password'],
               'db_host_name': pc_info['vps1_remote_hostname'],
               'db_port': pc_info['vps1_remote_port']}

    elif pc_name == 'server_remote_access_from_vps':
        res = {'db_name': database_name,
               'db_username': pc_info['server_remote_username_vps'],
               'db_user_password': pc_info['server_remote_password_vps'],
               'db_host_name': pc_info['server_remote_hostname'],
               'db_port': pc_info['server_remote_port']}

    # lan ----------------------
    elif pc_name == 'server_lan_access':
        res = {'db_name': database_name,
               'db_username': pc_info['server_lan_username'],
               'db_user_password': pc_info['server_lan_password'],
               'db_host_name': pc_info['server_lan_hostname'],
               'db_port': pc_info['server_lan_port']}

    return res


if __name__ == '__main__':

    print(get_database_info(vps1_local_access, 'test'))
    print(get_database_info(vps1_remote_access, 'test'))
