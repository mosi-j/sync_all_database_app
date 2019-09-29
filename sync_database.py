from database import DataBase
from my_time import get_now_time_string, get_now_time_second
import threading
from multi_thread_database import select_process
from time import sleep

from multi_thread_database import transfer_new_record

class SyncDataBase:
    def __init__(self, source_db_info, destination_db_info, max_packet_size, commit_mod, sync_period_time, rule_table, clean_sync=False):
        self.source_db_info = source_db_info
        self.destination_db_info = destination_db_info
        self.max_packet_size = max_packet_size
        self.commit_mod = commit_mod
        self.sync_period_time = sync_period_time
        self.rule_table = rule_table
        self.clean_sync = clean_sync

        self.source_db = DataBase(db_info=self.source_db_info)
        self.destination_db = DataBase(db_info=self.destination_db_info)
        self.source_last_sync_update_time = -1
        self.destination_connection = None

        self.last_sync_time = 0
        self.database_name = ''

    def get_source_update_time(self):
        query = 'select update_time from db_setting '
        args = ()
        source_update_time, error = self.source_db.select_query(query, args, True)
        if error is not None:
            return None, error

        return int(source_update_time[0][0]), None

    def get_destination_update_time(self):
        query = 'select update_time from db_setting '
        args = ()
        destination_update_time, error = self.destination_db.select_query(query, args, True)
        if error is not None:
            return None, error

        return int(destination_update_time[0][0]), None

    def set_destination_update_time(self, update_time):

        query = 'update db_setting set update_time=%s'
        args = (update_time)

        error = self.destination_db.command_query(query, args, True)
        if error is not None:
            return error

        return None

    def run(self, force=False):
        print('====================================')
        print(get_now_time_string())
        print('start sync database: {}'.format(self.database_name))
        print('====================================')

        current_destination_update_time = 0

        if force is True:
            # get source update time
            current_source_update_time, error = self.get_source_update_time()
            if error is not None:
                return 0, 0, error
        else:
            # check time period
            if self.last_sync_time + self.sync_period_time > get_now_time_second():
                print('sync in sleep time')
                self.last_sync_time = get_now_time_second()
                return 0, 0, None

            # get source update time
            current_source_update_time, error = self.get_source_update_time()
            if error is not None:
                return 0, 0, error
            # check source have new data
            if current_source_update_time <= self.source_last_sync_update_time:
                print('no new data')
                self.last_sync_time = get_now_time_second()
                return 0, 0, None

            # get destination update time
            current_destination_update_time, error = self.get_destination_update_time()
            if error is not None:
                return 0, 0, error

            if current_destination_update_time >= current_source_update_time:
                print('no need sync')
                self.last_sync_time = get_now_time_second()
                return 0, 0, None

        total, done, error = self.run_sync_rule()

        if error is not None:
            return total, done, error

        if done > 0:
            error = self.set_destination_update_time(update_time=current_source_update_time)
            if error is not None:
                return total, done, error

        if current_destination_update_time < current_source_update_time:
            error = self.set_destination_update_time(update_time=current_source_update_time)
            if error is not None:
                return total, done, error

        self.last_sync_time = get_now_time_second()
        self.source_last_sync_update_time = current_source_update_time

        return total, done, error

    def clean_table(self, database_obj, table_name, where_str=None):
        if where_str is None:
            query = 'delete from {0} where 1}'.format(table_name)
        else:
            query = 'delete from {0} where {1}}'.format(table_name, where_str)

        args = ()

        return database_obj.command_query(query, args, True)

    def update_table(self, source_table_name, destination_table_name, pk_field, commit_mod, destination_connection=None, where_str=None):
        new_record_count = 0
        total_transfer_record = 0
        error = None

        #start = get_now_time_second()

        if self.clean_sync is True:
            error = self.clean_table(self.destination_db, destination_table_name, where_str=where_str)
            if error is not None:
                return new_record_count, total_transfer_record, error

        select_max_thread = 5
        insert_max_thread = 2
        select_max_offset = 100000
        insert_max_offset = 100000
        lock = threading.Lock()

        new_record_count, total_transfer_record, error, run_time = transfer_new_record(
                                    source_db_info=self.source_db_info, destination_db_info=self.destination_db_info,
                                    source_table_name=source_table_name, destination_table_name=destination_table_name,
                                    select_max_thread=select_max_thread, insert_max_thread=insert_max_thread,
                                    select_max_offset=select_max_offset, insert_max_offset=insert_max_offset,
                                    lock=lock, where_str=where_str)

        # print(get_now_time_second() - start)

        return new_record_count, total_transfer_record, error, run_time

    def update_table2(self, source_table_name, destination_table_name, pk_field, commit_mod, destination_connection=None, where_str=None):
        # max_transfer_record = 40
        max_destination_record = self.max_packet_size
        destination_con = None
        new_record_count = 0
        total_transfer_record = 0

        destination_pk_list = None
        new_record_pk_list = None

        pk_count = pk_field.split()
        pk_count = len(pk_count)

        if self.clean_sync is True:
            error = self.clean_table(self.destination_db, destination_table_name)
            if error is not None:
                return new_record_count, total_transfer_record, error

        if commit_mod == 'no_commit':
            if destination_connection is None:
                error = 'connection error'
                return new_record_count, total_transfer_record, error
            else:
                destination_con = destination_connection






        # =======================
        source_record_count, err = self.source_db.get_record_count(source_table_name, where_str=where_str)
        if err is not None:
            return new_record_count, total_transfer_record, err

        print('source_pk_count: {}'.format(source_record_count))


        transfer_data = dict()

        destination_max_thread = 5
        source_max_thread = 5
        destination_offset = 20000
        source_offset = 20000

        if where_str is None:
            #destination_query = 'select {0} from {1}'.format(pk_field, destination_table_name)
            source_query = 'select {0} from {1}'.format(pk_field, source_table_name)
        else:
            #destination_query = 'select {0} from {1} where {2}'.format(pk_field, destination_table_name, where_str)
            source_query = 'select {0} from {1} where {2}'.format(pk_field, source_table_name, where_str)

        args = ()


        select_thread = threading.Thread(name='2', target=select_process,
                                         args=(transfer_data, self.source_db_info, source_max_thread, source_record_count, source_offset, source_query, args))

        destination_thread.start()
        while not destination_thread.is_alive():
            sleep(0.1)




        # --------------
        print('main step 1: get new pk list')
        # --------------
        print('step 1: get destination record count')

        destination_pk_count, err = self.destination_db.get_record_count(destination_table_name, where_str=where_str)
        if err is not None:
            return new_record_count, total_transfer_record, err

        print('destination_pk_count: {}'.format(destination_pk_count))

        # if destination_pk_count > max_destination_record:
        if destination_pk_count > 0:  # destination have record
            # print('step 2: get destination pk list')
            # print('step 2: get source pk list')

            # print('step 2: get source record count')
            source_pk_count, err = self.source_db.get_record_count(source_table_name, where_str=where_str)
            if err is not None:
                return new_record_count, total_transfer_record, err

            print('source_pk_count: {}'.format(source_pk_count))
            # -----------------
            print('step 2: get new pk list in multi thread function')

            destination_result = dict()
            source_result = dict()
            destination_max_thread = 5
            source_max_thread = 5
            destination_offset = 20000
            source_offset = 20000

            if where_str is None:
                destination_query = 'select {0} from {1}'.format(pk_field, destination_table_name)
                source_query = 'select {0} from {1}'.format(pk_field, source_table_name)
            else:
                destination_query = 'select {0} from {1} where {2}'.format(pk_field, destination_table_name, where_str)
                source_query = 'select {0} from {1} where {2}'.format(pk_field, source_table_name, where_str)

            args = ()

            destination_thread = threading.Thread(name='1', target=select_process, args=(destination_result, self.destination_db_info, destination_max_thread, destination_pk_count, destination_offset, destination_query, args))
            source_thread = threading.Thread(name='2', target=select_process, args=(source_result, self.source_db_info, source_max_thread, source_pk_count, source_offset, source_query, args))

            destination_thread.start()
            while not destination_thread.is_alive():
                sleep(0.1)

            source_thread.start()
            while not source_thread.is_alive():
                sleep(0.1)

            print('1 ---------')
            while threading.active_count() > 1:
                # print('result1: ' + str(len(result1['result'])))
                # print('result2 :' + str(len(result2['result'])))
                if len(source_result['result']) > 0 and len(destination_result['result']) > 0:
                    for item in source_result['result']:
                        if item in destination_result['result']:
                            destination_result['result'].remove(item)
                            source_result['result'].remove(item)

                #if len(source_result['result']) > 0:
                #    item = source_result['result'][-1]
                #    if item in destination_result['result']:
                #        destination_result['result'].remove(item)
                #        source_result['result'].remove(item)
                else:
                    sleep(0.1)

            print('2 ---------')
            for t in threading.enumerate():
                if t != threading.main_thread():
                    t.join()
            print('3 ---------')

            print(len(destination_result['result']))
            print(len(source_result['result']))

            for item in source_result['result']:
                if item in destination_result['result']:
                    destination_result['result'].remove(item)
                    source_result['result'].remove(item)
            print('4 ---------')

            new_record_pk_list = source_result['result']
            new_record_pk_list = tuple(new_record_pk_list)

            if pk_count == 1:
                l = list()
                for item in new_record_pk_list:
                    l.append(item[0])
                new_record_pk_list = tuple(l)
            print('5 ---------')

            new_record_count = len(new_record_pk_list)

        else:  # destination have not record
            print('step 2: get new record count')
            new_record_count, err = self.source_db.get_record_count(source_table_name, where_str=where_str)
            if err is not None:
                return new_record_count, total_transfer_record, err


        print('new_record_count: {}'.format(new_record_count))

        if new_record_count == 0:  # no new record
            return new_record_count, total_transfer_record, None
        # -----------------------------
        print('main step 2: transfer new record to destination')

        if commit_mod == 'all_commit':
            destination_con, err = self.destination_db.get_connection()

        start_index = 0

        loop = 0
        while True:
            if start_index >= new_record_count:
                break
            loop += 1

            if commit_mod == 'batch_commit':
                destination_con, err = self.destination_db.get_connection()

            # get new record from source
            print('step 3: get new record data list loop: {0} total / done : {1}/{2}'.format(loop, new_record_count, total_transfer_record))

            if destination_pk_count > 0:  # destination have record
                new_record = new_record_pk_list[start_index:start_index + self.max_packet_size]
                if where_str is None:
                    query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
                else:
                    query = 'select * from {0} where {3} and ({1}) in {2}'.format(source_table_name, pk_field, new_record, where_str)

                if len(new_record) == 1:
                    query = query[:-2] + ')'

            else:  # destination have not record
                if where_str is None:
                    query = 'select * from {0} limit {1}, {2}'.format(source_table_name, start_index, self.max_packet_size)
                else:
                    query = 'select * from {0} where {3} limit {1}, {2}'.format(source_table_name, start_index, self.max_packet_size, where_str)

            args = ()
            new_record, err = self.source_db.select_query(query, args, 1)
            if err is not None:
                return new_record_count, total_transfer_record, err

            v = '%s, ' * len(new_record[0])
            v = v.strip(', ')
            # transfer to destination
            print('step 3: set new record data list loop: {0} total / done : {1}/{2}'.format(loop, new_record_count, total_transfer_record))

            query = 'insert ignore into {0} values ({1})'.format(destination_table_name, v)
            args = (new_record)

            try:
                db = destination_con.cursor()
                db._defer_warnings = True
                db.autocommit = False
                db.executemany(query, args)
                if commit_mod == 'batch_commit':
                    destination_con.commit()
                    destination_con.close()
                    total_transfer_record += len(new_record)
                start_index += self.max_packet_size

            except Exception as e:
                try:
                    if destination_con.open is True:
                        destination_con.rollback()
                        destination_con.close()
                finally:
                    # error = 'cant execute command_query_many: {}'.format(str(e))
                    return new_record_count, total_transfer_record, str(e)

        if commit_mod == 'all_commit':
            destination_con.commit()
            destination_con.close()
            total_transfer_record = new_record_count

        if commit_mod == 'no_commit':
            total_transfer_record = new_record_count

        return new_record_count, total_transfer_record, None

    def update_table1(self, source_table_name, destination_table_name, pk_field, commit_mod, destination_connection=None, where_str=None):
        # max_transfer_record = 40
        max_destination_record = self.max_packet_size
        destination_con = None
        new_record_count = 0
        total_transfer_record = 0

        destination_pk_list = None
        new_record_pk_list = None

        pk_count = pk_field.split()
        pk_count = len(pk_count)

        if self.clean_sync is True:
            error = self.clean_table(self.destination_db, destination_table_name)
            if error is not None:
                return new_record_count, total_transfer_record, error

        if commit_mod == 'no_commit':
            if destination_connection is None:
                error = 'connection error'
                return new_record_count, total_transfer_record, error
            else:
                destination_con = destination_connection

        # --------------
        print('main step 1: get new pk list')
        # --------------
        print('step 1: get destination record count')

        destination_pk_count, err = self.destination_db.get_record_count(destination_table_name, where_str=where_str)
        if err is not None:
            return new_record_count, total_transfer_record, err

        print('destination_pk_count: {}'.format(destination_pk_count))

        # if destination_pk_count > max_destination_record:
        if destination_pk_count > 0:  # destination have record
            # print('step 2: get destination pk list')
            # print('step 2: get source pk list')

            # print('step 2: get source record count')
            source_pk_count, err = self.source_db.get_record_count(source_table_name, where_str=where_str)
            if err is not None:
                return new_record_count, total_transfer_record, err

            print('source_pk_count: {}'.format(source_pk_count))
            # -----------------
            print('step 2: get new pk list in multi thread function')

            destination_result = dict()
            source_result = dict()
            destination_max_thread = 5
            source_max_thread = 5
            destination_offset = 20000
            source_offset = 20000

            if where_str is None:
                destination_query = 'select {0} from {1}'.format(pk_field, destination_table_name)
                source_query = 'select {0} from {1}'.format(pk_field, source_table_name)
            else:
                destination_query = 'select {0} from {1} where {2}'.format(pk_field, destination_table_name, where_str)
                source_query = 'select {0} from {1} where {2}'.format(pk_field, source_table_name, where_str)

            args = ()

            destination_thread = threading.Thread(name='1', target=select_process, args=(destination_result, self.destination_db_info, destination_max_thread, destination_pk_count, destination_offset, destination_query, args))
            source_thread = threading.Thread(name='2', target=select_process, args=(source_result, self.source_db_info, source_max_thread, source_pk_count, source_offset, source_query, args))

            destination_thread.start()
            while not destination_thread.is_alive():
                sleep(0.1)

            source_thread.start()
            while not source_thread.is_alive():
                sleep(0.1)

            print('1 ---------')
            while threading.active_count() > 1:
                # print('result1: ' + str(len(result1['result'])))
                # print('result2 :' + str(len(result2['result'])))
                if len(source_result['result']) > 0 and len(destination_result['result']) > 0:
                    for item in source_result['result']:
                        if item in destination_result['result']:
                            destination_result['result'].remove(item)
                            source_result['result'].remove(item)

                #if len(source_result['result']) > 0:
                #    item = source_result['result'][-1]
                #    if item in destination_result['result']:
                #        destination_result['result'].remove(item)
                #        source_result['result'].remove(item)
                else:
                    sleep(0.1)

            print('2 ---------')
            for t in threading.enumerate():
                if t != threading.main_thread():
                    t.join()
            print('3 ---------')

            print(len(destination_result['result']))
            print(len(source_result['result']))

            for item in source_result['result']:
                if item in destination_result['result']:
                    destination_result['result'].remove(item)
                    source_result['result'].remove(item)
            print('4 ---------')

            new_record_pk_list = source_result['result']
            new_record_pk_list = tuple(new_record_pk_list)

            if pk_count == 1:
                l = list()
                for item in new_record_pk_list:
                    l.append(item[0])
                new_record_pk_list = tuple(l)
            print('5 ---------')

            new_record_count = len(new_record_pk_list)

        else:  # destination have not record
            print('step 2: get new record count')
            new_record_count, err = self.source_db.get_record_count(source_table_name, where_str=where_str)
            if err is not None:
                return new_record_count, total_transfer_record, err


        print('new_record_count: {}'.format(new_record_count))

        if new_record_count == 0:  # no new record
            return new_record_count, total_transfer_record, None
        # -----------------------------
        print('main step 2: transfer new record to destination')

        if commit_mod == 'all_commit':
            destination_con, err = self.destination_db.get_connection()

        start_index = 0

        loop = 0
        while True:
            if start_index >= new_record_count:
                break
            loop += 1

            if commit_mod == 'batch_commit':
                destination_con, err = self.destination_db.get_connection()

            # get new record from source
            print('step 3: get new record data list loop: {0} total / done : {1}/{2}'.format(loop, new_record_count, total_transfer_record))

            if destination_pk_count > 0:  # destination have record
                new_record = new_record_pk_list[start_index:start_index + self.max_packet_size]
                if where_str is None:
                    query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
                else:
                    query = 'select * from {0} where {3} and ({1}) in {2}'.format(source_table_name, pk_field, new_record, where_str)

                if len(new_record) == 1:
                    query = query[:-2] + ')'

            else:  # destination have not record
                if where_str is None:
                    query = 'select * from {0} limit {1}, {2}'.format(source_table_name, start_index, self.max_packet_size)
                else:
                    query = 'select * from {0} where {3} limit {1}, {2}'.format(source_table_name, start_index, self.max_packet_size, where_str)

            args = ()
            new_record, err = self.source_db.select_query(query, args, 1)
            if err is not None:
                return new_record_count, total_transfer_record, err

            v = '%s, ' * len(new_record[0])
            v = v.strip(', ')
            # transfer to destination
            print('step 3: set new record data list loop: {0} total / done : {1}/{2}'.format(loop, new_record_count, total_transfer_record))

            query = 'insert ignore into {0} values ({1})'.format(destination_table_name, v)
            args = (new_record)

            try:
                db = destination_con.cursor()
                db._defer_warnings = True
                db.autocommit = False
                db.executemany(query, args)
                if commit_mod == 'batch_commit':
                    destination_con.commit()
                    destination_con.close()
                    total_transfer_record += len(new_record)
                start_index += self.max_packet_size

            except Exception as e:
                try:
                    if destination_con.open is True:
                        destination_con.rollback()
                        destination_con.close()
                finally:
                    # error = 'cant execute command_query_many: {}'.format(str(e))
                    return new_record_count, total_transfer_record, str(e)

        if commit_mod == 'all_commit':
            destination_con.commit()
            destination_con.close()
            total_transfer_record = new_record_count

        if commit_mod == 'no_commit':
            total_transfer_record = new_record_count

        return new_record_count, total_transfer_record, None

    def update_table0(self, source_table_name, destination_table_name, pk_field, commit_mod, destination_connection=None, where_str=None):
        # max_transfer_record = 40
        max_destination_record = 10000000
        destination_con = None
        new_record_count = 0
        total_transfer_record = 0

        destination_pk_list = None
        new_record_pk_list = None

        pk_count = pk_field.split()
        pk_count = len(pk_count)

        if self.clean_sync is True:
            error = self.clean_table(self.destination_db, destination_table_name)
            if error is not None:
                return new_record_count, total_transfer_record, error

        if commit_mod == 'no_commit':
            if destination_connection is None:
                error = 'connection error'
                return new_record_count, total_transfer_record, error
            else:
                destination_con = destination_connection

        # --------------
        print('step 1: get destination record count')

        destination_pk_count, err = self.destination_db.get_record_count(destination_table_name, where_str=where_str)
        if err is not None:
            return new_record_count, total_transfer_record, err

        print('destination_pk_count: {}'.format(destination_pk_count))

        # ---------------
        if destination_pk_count > 0:
            print('step 2: get destination pk list')
            if where_str is None:
                query = 'select {0} from {1}'.format(pk_field, destination_table_name)
            else:
                query = 'select {0} from {1} where {2}'.format(pk_field, destination_table_name, where_str)

            args = ()

            destination_pk_list, err = self.destination_db.select_query(query, args, 1)
            if err is not None:
                return new_record_count, total_transfer_record, err

            #if pk_count == 1:
            #    l = list()
            #    for item in destination_pk_list:
            #        l.append(item[0])
            #    destination_pk_list = tuple(l)

        # ---------------
        if destination_pk_count > max_destination_record:
            print('step 2: get new record pk list. (destination_pk_count > max_destination_record)')
            if where_str is None:
                query = 'select {0} from {1}'.format(pk_field, source_table_name)
            else:
                query = 'select {0} from {1} where {2}'.format(pk_field, source_table_name, where_str)
            args = ()
            source_pk_list, err = self.source_db.select_query(query, args, 1)
            if err is not None:
                return new_record_count, total_transfer_record, err

            #if pk_count == 1:
            #    l = list()
            #    for item in source_pk_list:
            #        l.append(item[0])
            #    source_pk_list = tuple(l)
            # ------------------
            destination_pk_list = list(destination_pk_list)
            source_pk_list = list(source_pk_list)
            new_record_pk_list = list()

            print('source pk count: {}'.format(len(source_pk_list)))

            while len(source_pk_list) > 0:
                pk = source_pk_list.pop()
                if pk not in destination_pk_list:
                    new_record_pk_list.append(pk)

                destination_pk_list.remove(pk)

            if pk_count == 1:
                l = list()
                for item in new_record_pk_list:
                    l.append(item[0])
                new_record_pk_list = tuple(l)

            new_record_count = len(new_record_pk_list)
            # new_record_pk_list = tuple(new_record_pk_list)

        elif destination_pk_count > 0:  # destination have record
            print('step 2: get new record pk list. (max_destination_record > destination_pk_count > 0)')

            if pk_count == 1:
                l = list()
                for item in destination_pk_list:
                    l.append(item[0])
                destination_pk_list = tuple(l)

            if where_str is None:
                query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name,
                                                                            destination_pk_list)
            else:
                query = 'select {0} from {1} where {3} and ({0}) not in {2}'.format(pk_field, source_table_name,
                                                                                    destination_pk_list, where_str)

            if len(destination_pk_list) == 1:
                query = query[:-2] + ')'

            args = ()
            new_record_pk_list, err = self.source_db.select_query(query, args, 1)
            if err is not None:
                return new_record_count, total_transfer_record, err


            if pk_count == 1:
                l = list()
                for item in new_record_pk_list:
                    l.append(item[0])
                new_record_pk_list = tuple(l)

            new_record_count = len(new_record_pk_list)

        else:  # destination have not record
            print('step 2: get new record count')
            new_record_count, err = self.source_db.get_record_count(source_table_name, where_str=where_str)
            if err is not None:
                return new_record_count, total_transfer_record, err

            #if where_str is None:
            #    query = 'select count(*) from {0}'.format(source_table_name)
            #else:
            #    query = 'select count(*) from {0} where {1}'.format(source_table_name, where_str)
            #args = ()
            #new_record_count, err = self.source_db.select_query(query, args, 1)
            #if err is not None:
            #    return new_record_count, total_transfer_record, err
            #new_record_count = int(new_record_count[0][0])

        # ------------


        # --------------
        # --------------
        #print('step 1: get destination pk list')
        #if where_str is None:
        #    query = 'select {0} from {1}'.format(pk_field, destination_table_name)
        #else:
        #    query = 'select {0} from {1} where {2}'.format(pk_field, destination_table_name, where_str)

        #args = ()

        #destination_pk_list, err = self.destination_db.select_query(query, args, 1)
        #if err is not None:
        #    return new_record_count, total_transfer_record, err

        #if pk_count == 1:
        #    l = list()
        #    for item in destination_pk_list:
        #        l.append(item[0])
        #    destination_pk_list = tuple(l)

        # --------------
        #if len(destination_pk_list) > 0:  # destination have record
        #    print('step 2: get new record pk list')

        #    if where_str is None:
        #        query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_pk_list)
        #    else:
        #        query = 'select {0} from {1} where {3} and ({0}) not in {2}'.format(pk_field, source_table_name, destination_pk_list, where_str)

        #    if len(destination_pk_list) == 1:
        #        query = query[:-2] + ')'

        #    args = ()
        #    new_record_pk_list, err = self.source_db.select_query(query, args, 1)
        #    if err is not None:
        #        return new_record_count, total_transfer_record, err

        #    if pk_count == 1:
        #        l = list()
        #        for item in new_record_pk_list:
        #            l.append(item[0])
        #        new_record_pk_list = tuple(l)

        #    new_record_count = len(new_record_pk_list)

        #else:  # destination have not record
        #    print('step 2: get new record count')

        #    if where_str is None:
        #        query = 'select count(*) from {0}'.format(source_table_name)
        #    else:
        #        query = 'select count(*) from {0} where {1}'.format(source_table_name, where_str)
        #    args = ()
        #    new_record_count, err = self.source_db.select_query(query, args, 1)
        #    if err is not None:
        #        return new_record_count, total_transfer_record, err

        #    new_record_count = int(new_record_count[0][0])

        # ------------
        print('new_record_count: {}'.format(new_record_count))

        if new_record_count == 0:  # no new record
            return new_record_count, total_transfer_record, None

        if commit_mod == 'all_commit':
            destination_con, err = self.destination_db.get_connection()

        start_index = 0

        loop = 0
        while True:
            if start_index >= new_record_count:
                break
            loop += 1

            if commit_mod == 'batch_commit':
                destination_con, err = self.destination_db.get_connection()

            # get new record from source
            print('step 3: get new record data list loop: {0} total / done : {1}/{2}'.format(loop, new_record_count, total_transfer_record))

            if destination_pk_count > 0:  # destination have record
                new_record = new_record_pk_list[start_index:start_index + self.max_packet_size]
                if where_str is None:
                    query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
                else:
                    query = 'select * from {0} where {3} and ({1}) in {2}'.format(source_table_name, pk_field, new_record, where_str)

                if len(new_record) == 1:
                    query = query[:-2] + ')'

            else:  # destination have not record
                if where_str is None:
                    query = 'select * from {0} limit {1}, {2}'.format(source_table_name, start_index, self.max_packet_size)
                else:
                    query = 'select * from {0} where {3} limit {1}, {2}'.format(source_table_name, start_index, self.max_packet_size, where_str)

            args = ()
            new_record, err = self.source_db.select_query(query, args, 1)
            if err is not None:
                return new_record_count, total_transfer_record, err

            v = '%s, ' * len(new_record[0])
            v = v.strip(', ')
            # transfer to destination
            print('step 3: set new record data list loop: {0} total / done : {1}/{2}'.format(loop, new_record_count, total_transfer_record))

            query = 'insert ignore into {0} values ({1})'.format(destination_table_name, v)
            args = (new_record)

            try:
                db = destination_con.cursor()
                db._defer_warnings = True
                db.autocommit = False
                db.executemany(query, args)
                if commit_mod == 'batch_commit':
                    destination_con.commit()
                    destination_con.close()
                    total_transfer_record += len(new_record)
                start_index += self.max_packet_size

            except Exception as e:
                try:
                    if destination_con.open is True:
                        destination_con.rollback()
                        destination_con.close()
                finally:
                    # error = 'cant execute command_query_many: {}'.format(str(e))
                    return new_record_count, total_transfer_record, str(e)

        if commit_mod == 'all_commit':
            destination_con.commit()
            destination_con.close()
            total_transfer_record = new_record_count

        if commit_mod == 'no_commit':
            total_transfer_record = new_record_count

        return new_record_count, total_transfer_record, None

    def run_sync_rule(self):
        return self.sync_rule(rule_table=self.rule_table)

    def sync_rule(self, rule_table):
        #from my_database_info import open_days, index_data, share_info
        destination_connection = None
        all_new_record_count = 0
        all_total_transfer_record = 0
        error = None

        #rul_table = [[open_days, open_days, None],
        #             [index_data, index_data, "en_index_12_digit_code <> 'IRX6X01T0007'"],
        #             [share_info, share_info, None]]
        # rule_table = [[share_info,share_info]]

        if self.commit_mod == 'all_database':
            commit_mod = 'no_commit'

        elif self.commit_mod == 'each_pocket':
            commit_mod = 'batch_commit'

        elif self.commit_mod == 'each_table':
            commit_mod = 'all_commit'
        else:
            error = 'invalid commit_mod'
            return all_new_record_count, all_total_transfer_record, error

        if self.commit_mod == 'all_database':
            destination_connection, error = self.destination_db.get_connection()
            if error is not None:
                return all_new_record_count, all_total_transfer_record, error

        for table in rule_table:
            # --------------------
            print('-----------------------------------')
            print(table[0]['name'])
            print('-----------------------------------')
            new_record_count, total_transfer_record, error, run_time = self.update_table(source_table_name=table[0]['name'],
                                                                               destination_table_name=table[1]['name'],
                                                                               pk_field=table[1]['pk_field'],
                                                                               commit_mod=commit_mod,
                                                                               destination_connection=destination_connection,
                                                                               where_str=table[2])
            for err in error:
                if err is not None:
            #if error is not None:
                    print(error)
                    if self.commit_mod == 'all_database':
                        try:
                            if destination_connection.open is True:
                                destination_connection.rollback()
                                destination_connection.close()
                                all_new_record_count = 0
                                all_total_transfer_record = 0

                        finally:
                            return all_new_record_count, all_total_transfer_record, error
                    return all_new_record_count, all_total_transfer_record, error

            all_new_record_count += new_record_count
            all_total_transfer_record += total_transfer_record

        # --------------------
        if self.commit_mod == 'all_database':
            try:
                destination_connection.commit()
                destination_connection.close()
            except Exception as e:
                error = str(e)

        return all_new_record_count, all_total_transfer_record, error

    def sync_rule0(self, rule_table):
        #from my_database_info import open_days, index_data, share_info
        destination_connection = None
        all_new_record_count = 0
        all_total_transfer_record = 0
        error = None

        #rul_table = [[open_days, open_days, None],
        #             [index_data, index_data, "en_index_12_digit_code <> 'IRX6X01T0007'"],
        #             [share_info, share_info, None]]
        # rule_table = [[share_info,share_info]]

        if self.commit_mod == 'all_database':
            commit_mod = 'no_commit'

        elif self.commit_mod == 'each_pocket':
            commit_mod = 'batch_commit'

        elif self.commit_mod == 'each_table':
            commit_mod = 'all_commit'
        else:
            error = 'invalid commit_mod'
            return all_new_record_count, all_total_transfer_record, error

        if self.commit_mod == 'all_database':
            destination_connection, error = self.destination_db.get_connection()
            if error is not None:
                return all_new_record_count, all_total_transfer_record, error

        for table in rule_table:
            # --------------------
            print('-----------------------------------')
            print(table[0]['name'])
            print('-----------------------------------')
            new_record_count, total_transfer_record, error = self.update_table(source_table_name=table[0]['name'],
                                                                               destination_table_name=table[1]['name'],
                                                                               pk_field=table[1]['pk_field'],
                                                                               commit_mod=commit_mod,
                                                                               destination_connection=destination_connection,
                                                                               where_str=table[2])
            if error is not None:
                print(error)
                if self.commit_mod == 'all_database':
                    try:
                        if destination_connection.open is True:
                            destination_connection.rollback()
                            destination_connection.close()
                            all_new_record_count = 0
                            all_total_transfer_record = 0

                    finally:
                        return all_new_record_count, all_total_transfer_record, error
                return all_new_record_count, all_total_transfer_record, error

            all_new_record_count += new_record_count
            all_total_transfer_record += total_transfer_record

        # --------------------
        if self.commit_mod == 'all_database':
            try:
                destination_connection.commit()
                destination_connection.close()
            except Exception as e:
                error = str(e)

        return all_new_record_count, all_total_transfer_record, error

    def set_rull_table(self, rule_table):
        self.rule_table = rule_table


if __name__ == '__main__':
    source_db_info1 = {'db_name': 'new_bourse_client_0.1',
                      'db_username': 'bourse_user',
                      'db_user_password': 'Asdf1234',
                      'db_host_name': 'localhost',
                      'db_port': 3306}

    destination_db_info1 = {'db_name': 'tsetmc_raw_data',
                           'db_username': 'bourse_user',
                           'db_user_password': 'Asdf1234',
                           'db_host_name': 'localhost',
                           'db_port': 3306}

    max_packet_size = 40
    commit_mod1 = 'each_table'
    sync_period_time = 10

    from my_database_info import open_days, index_data, share_info

    rule_table = [[open_days, open_days, None],
                  [index_data, index_data, "en_index_12_digit_code <> 'IRX6X01T0007'"],
                  [share_info, share_info, None]]

    sync = SyncDataBase(source_db_info1, destination_db_info1, max_packet_size, commit_mod1, sync_period_time, rule_table)







class SyncDataBase1:
    def __init__(self, source_db_info, destination_db_info, max_packet_size, transaction_mod, sync_period_time):
        self.source_db_info = source_db_info
        self.destination_db_info = destination_db_info
        self.max_packet_size = max_packet_size
        self.transaction_mod = transaction_mod
        self.sync_period_time = sync_period_time

        self.source_db = DataBase(db_info=self.source_db_info)
        self.destination_db = DataBase(db_info=self.destination_db_info)
        self.source_last_sync_update_time = -1
        self.destination_connection = None

        self.last_sync_time = 0
        self.database_name = ''

    def sync_table(self, source_table_name, destination_table_name, pk_field, is_multi_pk, autocommit_each_table=True):
        if self.transaction_mod is True:
            if self.destination_connection is None:
                try:
                    self.destination_connection, err = self.destination_db.get_connection()
                    if err is not None:
                        raise Exception(err)

                except Exception as e:
                    try:
                        if self.destination_connection.open is True:
                            self.destination_connection.rollback()
                            self.destination_connection.close()
                    finally:
                        error = 'cant create destination_con: {}'.format(str(e))
                        self.destination_connection = None
                        return error

        # get destination pk records
        query = 'select {0} from {1}'.format(pk_field, destination_table_name)
        args = ()
        destination_pk, error = self.destination_db.select_query(query, args, True)
        if error is not None:
            return error
        # print('destination_pk')
        # print(query)
        # print(destination_pk)
        if is_multi_pk is False:
            l = list()
            for item in destination_pk:
                l.append(item[0])

            destination_pk = tuple(l)




        if len(destination_pk) > 0:
            if is_multi_pk is True:
                query = 'select count(*) from {0} where ({1}) not in {2}'.format(source_table_name, pk_field, destination_pk)
            else:
                query = 'select count(*) from {0} where {1} not in {2}'.format(source_table_name, pk_field, destination_pk)
        else:
            query = 'select count(*) from {0}'.format(source_table_name)

        args = ()
        new_record_count, error = self.source_db.select_query(query, args, True)
        # print('new_record_count')
        # print(query)
        # print(new_record_count)

        if error is not None:
            return error

        new_record_count = int(new_record_count[0][0])
        print(new_record_count)

        loop_start = 0
        loop_offset = self.max_packet_size
        loop = 0
        while loop_start < new_record_count:
            loop += 1
            print('time: {0}:=> start get data from source loop:{1}'.format(get_now_time_string(), loop))
            if len(destination_pk) > 0:
                if is_multi_pk is True:
                    query = 'select * from {0} where ({1}) not in {2} order by {1} limit {3}, {4}'\
                        .format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)
                else:
                    query = 'select * from {0} where {1} not in {2} order by {1} limit {3}, {4}'\
                        .format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)
            else:
                query = 'select * from {0} order by {1} limit {2}, {3}'\
                    .format(source_table_name, pk_field, loop_start, loop_offset)

            args = ()
            source_data, error = self.source_db.select_query(query, args, True)
            if error is not None:
                return error
            print('time: {0}:=> end get data from source loop:{1}'.format(get_now_time_string(), loop))

            # انتقال داده ها در بسته های کوچکتر به جدول مقصد
            print('time: {0}:=> start transfer data to destination table loop:{1} count:{2}'
                  .format(get_now_time_string(), loop, len(source_data)))
            if len(source_data) > 1:
                v = '%s, ' * len(source_data[0])
                v = v.strip(', ')
                query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
                args = list(source_data)

                # insert to destination
                if self.transaction_mod is False:
                    error = self.destination_db.command_query_many(query, args, True)
                    if error is not None:
                        return error

                else:
                    try:
                        db = self.destination_connection.cursor()
                        db._defer_warnings = True
                        db.autocommit = False
                        db.executemany(query, args)

                    except Exception as e:
                        try:
                            if self.destination_connection.open is True:
                                self.destination_connection.rollback()
                                self.destination_connection.close()
                                self.destination_connection = None

                        finally:
                            error = 'cant execute command_query_many: {}'.format(str(e))
                            return error
                            # return 'cant execute command_query_many: {}'.format(str(e))

            elif len(source_data) == 1:
                v = '%s, ' * len(source_data[0])
                v = v.strip(', ')
                query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
                args = list(source_data[0])

                # insert to destination
                if self.transaction_mod is False:
                    error = self.destination_db.command_query(query, args, True)
                    if error is not None:
                        return error
                else:
                    try:
                        db = self.destination_connection.cursor()
                        db._defer_warnings = True
                        db.autocommit = False
                        db.execute(query, args)

                    except Exception as e:
                        try:
                            if self.destination_connection.open is True:
                                self.destination_connection.rollback()
                                self.destination_connection.close()
                                self.destination_connection = None

                        finally:
                            error = 'cant execute command_query: {}'.format(str(e))
                            return error
                            # return 'cant execute command_query: {}'.format(str(e))

            print('time: {0}:=> end transfer data to destination table loop:{1} count:{2}'
                  .format(get_now_time_string(), loop, len(source_data)))

            loop_start += loop_offset

        if self.transaction_mod is True and autocommit_each_table is True:
            self.destination_connection.commit()
            self.destination_connection.close()
            self.destination_connection = None

        error = None
        return error

    def sync_table1(self, source_table_name, destination_table_name, pk_field, is_multi_pk, autocommit_each_table=True):
        if self.transaction_mod is True:
            if self.destination_connection is None:
                try:
                    self.destination_connection, err = self.destination_db.get_connection()
                    if err is not None:
                        raise Exception(err)

                except Exception as e:
                    try:
                        if self.destination_connection.open is True:
                            self.destination_connection.rollback()
                            self.destination_connection.close()
                    finally:
                        error = 'cant create destination_con: {}'.format(str(e))
                        self.destination_connection = None
                        return error

        # get destination pk records
        query = 'select {0} from {1}'.format(pk_field, destination_table_name)
        args = ()
        destination_pk, error = self.destination_db.select_query(query, args, True)
        if error is not None:
            return error
        # print('destination_pk')
        # print(query)
        # print(destination_pk)
        if is_multi_pk is False:
            l = list()
            for item in destination_pk:
                l.append(item[0])

            destination_pk = tuple(l)

        if len(destination_pk) > 0:
            if is_multi_pk is True:
                #query = 'select count(*) from {0} where ({1}) not in {2}'.format(source_table_name, pk_field, destination_pk)
                query = 'select count(*) from {0} where ({1}) not in %s'.format(source_table_name, pk_field)
            else:
                #query = 'select count(*) from {0} where {1} not in {2}'.format(source_table_name, pk_field, destination_pk)
                query = 'select count(*) from {0} where {1} not in %s'.format(source_table_name, pk_field)
            args = (destination_pk)


        else:
            query = 'select count(*) from {0}'.format(source_table_name)

            args = ()
        #args = ()
        new_record_count, error = self.source_db.select_query(query, args, True)
        # print('new_record_count')
        # print(query)
        # print(new_record_count)

        if error is not None:
            return error

        new_record_count = int(new_record_count[0][0])
        print(new_record_count)

        loop_start = 0
        loop_offset = self.max_packet_size
        loop = 0
        while loop_start < new_record_count:
            loop += 1
            print('time: {0}:=> start get data from source loop:{1}'.format(get_now_time_string(), loop))
            if len(destination_pk) > 0:
                if is_multi_pk is True:
                    #query = 'select * from {0} where ({1}) not in {2} order by {1} limit {3}, {4}'\
                    #    .format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)

                    query = 'select * from {0} where ({1}) not in %s order by {1} limit {3}, {4}'\
                        .format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)
                else:
                    #query = 'select * from {0} where {1} not in {2} order by {1} limit {3}, {4}'\
                    #    .format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)

                    query = 'select * from {0} where {1} not in %s order by {1} limit {3}, {4}'\
                        .format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)
                    args = (destination_pk)


            else:
                query = 'select * from {0} order by {1} limit {2}, {3}'\
                    .format(source_table_name, pk_field, loop_start, loop_offset)

                args = ()
            #args = ()
            source_data, error = self.source_db.select_query(query, args, True)
            if error is not None:
                return error
            print('time: {0}:=> end get data from source loop:{1}'.format(get_now_time_string(), loop))

            # انتقال داده ها در بسته های کوچکتر به جدول مقصد
            print('time: {0}:=> start transfer data to destination table loop:{1} count:{2}'
                  .format(get_now_time_string(), loop, len(source_data)))
            if len(source_data) > 1:
                v = '%s, ' * len(source_data[0])
                v = v.strip(', ')
                query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
                args = list(source_data)

                # insert to destination
                if self.transaction_mod is False:
                    error = self.destination_db.command_query_many(query, args, True)
                    if error is not None:
                        return error

                else:
                    try:
                        db = self.destination_connection.cursor()
                        db._defer_warnings = True
                        db.autocommit = False
                        db.executemany(query, args)

                    except Exception as e:
                        try:
                            if self.destination_connection.open is True:
                                self.destination_connection.rollback()
                                self.destination_connection.close()
                                self.destination_connection = None

                        finally:
                            error = 'cant execute command_query_many: {}'.format(str(e))
                            return error
                            # return 'cant execute command_query_many: {}'.format(str(e))

            elif len(source_data) == 1:
                v = '%s, ' * len(source_data[0])
                v = v.strip(', ')
                query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
                args = list(source_data[0])

                # insert to destination
                if self.transaction_mod is False:
                    error = self.destination_db.command_query(query, args, True)
                    if error is not None:
                        return error
                else:
                    try:
                        db = self.destination_connection.cursor()
                        db._defer_warnings = True
                        db.autocommit = False
                        db.execute(query, args)

                    except Exception as e:
                        try:
                            if self.destination_connection.open is True:
                                self.destination_connection.rollback()
                                self.destination_connection.close()
                                self.destination_connection = None

                        finally:
                            error = 'cant execute command_query: {}'.format(str(e))
                            return error
                            # return 'cant execute command_query: {}'.format(str(e))

            print('time: {0}:=> end transfer data to destination table loop:{1} count:{2}'
                  .format(get_now_time_string(), loop, len(source_data)))

            loop_start += loop_offset

        if self.transaction_mod is True and autocommit_each_table is True:
            self.destination_connection.commit()
            self.destination_connection.close()
            self.destination_connection = None

        error = None
        return error

    def sync_table0(self, source_table_name, destination_table_name, pk_field, is_multi_pk, autocommit=True):
        if self.transaction_mod is True:
            if self.destination_connection is None:
                try:
                    self.destination_connection, err = self.destination_db.get_connection()
                    if err is not None:
                        raise Exception(err)

                except Exception as e:
                    try:
                        if self.destination_connection.open is True:
                            self.destination_connection.rollback()
                            self.destination_connection.close()
                    finally:
                        error = 'cant create destination_con: {}'.format(str(e))
                        result = False
                        self.destination_connection = None
                        # print('cant create destination_con: {}'.format(str(e)))
                        return result, error

        # get destination pk records
        query = 'select {0} from {1} '.format(pk_field, destination_table_name)
        args = ()
        destination_pk, error = self.destination_db.select_query(query, args, True)
        if error is not None:
            return False, error

        if is_multi_pk is False:
            l = list()
            for item in destination_pk:
                l.append(item[0])

            destination_pk = tuple(l)

        if len(destination_pk) > 0:
            if is_multi_pk is True:
                query = 'select count(*) from {0} where ({1}) not in {2}'.format(source_table_name, pk_field, destination_pk)
            else:
                query = 'select count(*) from {0} where {1} not in {2}'.format(source_table_name, pk_field, destination_pk)

        else:
            query = 'select count(*) from {0}'.format(source_table_name)

        args = ()
        new_record_count, error = self.source_db.select_query(query, args, True)
        if error is not None:
            return False, error

        new_record_count = int(new_record_count[0][0])

        loop_start = 0
        loop_offset = self.max_packet_size
        loop = 0
        while loop_start < new_record_count:
            loop += 1
            print('time: {0}:=> start get data from source loop:{1}'.format(get_now_time_string(), loop))
            if len(destination_pk) > 0:
                if is_multi_pk is True:
                    query = 'select * from {0} where ({1}) not in {2} order by {1} limit {3}, {4}'\
                        .format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)
                else:
                    query = 'select * from {0} where {1} not in {2} order by {1} limit {3}, {4}'\
                        .format(source_table_name, pk_field, destination_pk, loop_start, loop_offset)

            else:
                query = 'select * from {0} order by {1} limit {2}, {3}'\
                    .format(source_table_name, pk_field, loop_start, loop_offset)

            args = ()
            source_data, error = self.source_db.select_query(query, args, True)
            if error is not None:
                return False, error
            print('time: {0}:=> end get data from source loop:{1}'.format(get_now_time_string(), loop))

            # انتقال داده ها در بسته های کوچکتر به جدول مقصد
            print('time: {0}:=> start transfer data to destination table loop:{1} count:{2}'
                  .format(get_now_time_string(), loop, len(source_data)))
            if len(source_data) > 1:
                v = '%s, ' * len(source_data[0])
                v = v.strip(', ')
                query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
                args = list(source_data)

                # insert to destination
                if self.transaction_mod is False:
                    error = self.destination_db.command_query_many(query, args, True)
                    if error is not None:
                        result = False
                        return result, error

                else:
                    try:
                        db = self.destination_connection.cursor()
                        db._defer_warnings = True
                        db.autocommit = False
                        db.executemany(query, args)

                    except Exception as e:
                        try:
                            if self.destination_connection.open is True:
                                self.destination_connection.rollback()
                                self.destination_connection.close()
                                self.destination_connection = None

                        finally:
                            error = 'cant execute command_query_many: {}'.format(str(e))
                            result = False
                            return result, error
                            # return 'cant execute command_query_many: {}'.format(str(e))

            elif len(source_data) == 1:
                v = '%s, ' * len(source_data[0])
                v = v.strip(', ')
                query = 'insert IGNORE into {0} values ({1})'.format(destination_table_name, v)
                args = list(source_data[0])

                # insert to destination
                if self.transaction_mod is False:
                    error = self.destination_db.command_query(query, args, True)
                    if error is not None:
                        result = False
                        return result, error
                else:
                    try:
                        db = self.destination_connection.cursor()
                        db._defer_warnings = True
                        db.autocommit = False
                        db.execute(query, args)

                    except Exception as e:
                        try:
                            if self.destination_connection.open is True:
                                self.destination_connection.rollback()
                                self.destination_connection.close()
                                self.destination_connection = None

                        finally:
                            error = 'cant execute command_query: {}'.format(str(e))
                            result = False
                            return result, error
                            # return 'cant execute command_query: {}'.format(str(e))

            print('time: {0}:=> end transfer data to destination table loop:{1} count:{2}'
                  .format(get_now_time_string(), loop, len(source_data)))

            loop_start += loop_offset

        if self.transaction_mod is True and autocommit is True:
            self.destination_connection.commit()
            self.destination_connection.close()
            self.destination_connection = None

        error = None
        result = True
        return result, error

    def get_source_update_time(self):
        query = 'select update_time from db_setting '
        args = ()
        source_update_time, error = self.source_db.select_query(query, args, True)
        if error is not None:
            return None, error

        return int(source_update_time[0][0]), None

    def get_destination_update_time(self):
        query = 'select update_time from db_setting '
        args = ()
        destination_update_time, error = self.destination_db.select_query(query, args, True)
        if error is not None:
            return None, error

        return int(destination_update_time[0][0]), None

    def set_destination_update_time(self, update_time, autocommit):
        if self.transaction_mod is True:
            if self.destination_connection is None:
                try:
                    self.destination_connection, err = self.destination_db.get_connection()
                    if err is not None:
                        raise Exception(err)

                except Exception as e:
                    try:
                        if self.destination_connection.open is True:
                            self.destination_connection.rollback()
                            self.destination_connection.close()
                    finally:
                        error = 'cant create destination_con: {}'.format(str(e))
                        self.destination_connection = None
                        return error

        query = 'update db_setting set update_time=%s'
        args = (update_time)

        if self.transaction_mod is False:
            error = self.destination_db.command_query(query, args, True)
            if error is not None:
                return error
        else:
            try:
                db = self.destination_connection.cursor()
                db._defer_warnings = True
                db.autocommit = False
                db.execute(query, args)

            except Exception as e:
                try:
                    if self.destination_connection.open is True:
                        self.destination_connection.rollback()
                        self.destination_connection.close()
                        self.destination_connection = None

                finally:
                    error = 'cant execute command_query: {}'.format(str(e))
                    return error

        if self.transaction_mod is True and autocommit is True:
            self.destination_connection.commit()
            self.destination_connection.close()
            self.destination_connection = None

        return None

    def run(self, autocommit_each_table, force=False):
        print('start sync database: {}'.format(self.database_name))

        if force is False:
            # check time period
            if self.last_sync_time + self.sync_period_time > get_now_time_second():
                print('sync in sleep time')
                self.last_sync_time = get_now_time_second()
                return None

            # get source update time
            current_source_update_time, error = self.get_source_update_time()
            if error is not None:
                return error
            # check source have new data
            if current_source_update_time <= self.source_last_sync_update_time:
                print('no new data')
                self.last_sync_time = get_now_time_second()
                return None

            # get destination update time
            current_destination_update_time, error = self.get_destination_update_time()
            if error is not None:
                return error

            if current_destination_update_time >= current_source_update_time:
                print('no need sync')
                self.last_sync_time = get_now_time_second()
                return None
        else:
            # get source update time
            current_source_update_time, error = self.get_source_update_time()
            if error is not None:
                return error

        # start sync
        if autocommit_each_table is True:
            autocommit = True
        else:
            autocommit = False

        error = self.sync_rule(autocommit=autocommit)
        if error is not None:
            return error

        # set update time
        error = self.set_destination_update_time(update_time=current_source_update_time, autocommit=autocommit)
        if error is not None:
            return error

        if autocommit_each_table is False:
            self.destination_connection.commit()
            self.destination_connection.close()
            self.destination_connection = None

        self.last_sync_time = get_now_time_second()
        self.source_last_sync_update_time = current_source_update_time

        return None

    def sync_rule(self, autocommit):
        pass


# define class for every sync select_process
class SyncSiteAppDataBase(SyncDataBase):

    def __init__(self, source_db_info, destination_db_info, max_packet_size, transaction_mod, sync_period_time):
        SyncDataBase.__init__(self, source_db_info, destination_db_info,
                              max_packet_size, transaction_mod, sync_period_time)
        self.database_name = 'SiteApp'

    def run0(self, all_table, force=False):
        if force is False:
            # check time period
            if self.last_sync_time + self.sync_period_time > get_now_time_second():
                print('sync in sleep time')
                self.last_sync_time = get_now_time_second()
                return None

            # get source update time
            current_source_update_time, error = self.get_source_update_time()
            if error is not None:
                return error
            # check source have new data€
            if current_source_update_time <= self.source_last_sync_update_time:
                print('no new data')
                self.last_sync_time = get_now_time_second()
                return None

            # get destination update time
            current_destination_update_time, error = self.get_destination_update_time()
            if error is not None:
                return error

            if current_destination_update_time >= current_source_update_time:
                print('no need sync')
                self.last_sync_time = get_now_time_second()
                return None
        else:
            # get source update time
            current_source_update_time, error = self.get_source_update_time()
            if error is not None:
                return error

        # start sync
        if all_table is True:
            autocommit = False
        else:
            autocommit = True

        print('open_days')
        table = 'open_days'
        pk_field = 'date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        print('shareholders_data_backup')
        table = 'shareholders_data_backup'
        pk_field = 'en_symbol_12_digit_code, date_m, sh_id'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        print('share_adjusted_data_backup')
        table = 'share_adjusted_data_backup'
        pk_field = 'en_symbol_12_digit_code, date_m, adjusted_type'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        # set update time
        error = self.set_destination_update_time(update_time=current_source_update_time, autocommit=autocommit)
        if error is not None:
            return error

        if all_table is True:
            self.destination_connection.commit()
            self.destination_connection.close()
            self.destination_connection = None

        self.last_sync_time = get_now_time_second()
        self.source_last_sync_update_time = current_source_update_time

        return None

    def sync_rule(self, autocommit):
        print('open_days')
        table = 'open_days'
        pk_field = 'date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        print('shareholders_data_backup')
        table = 'shareholders_data_backup'
        pk_field = 'en_symbol_12_digit_code, date_m, sh_id'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        print('share_adjusted_data_backup')
        table = 'share_adjusted_data_backup'
        pk_field = 'en_symbol_12_digit_code, date_m, adjusted_type'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        return None


class SyncBackTestAppDataBase(SyncDataBase):

    def __init__(self, source_db_info, destination_db_info, max_packet_size, transaction_mod, sync_period_time):
        SyncDataBase.__init__(self, source_db_info, destination_db_info,
                              max_packet_size, transaction_mod, sync_period_time)
        self.database_name = 'BackTestApp'

    def sync_rule(self, autocommit):
        print('open_days')
        table = 'open_days'
        pk_field = 'date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        print('shareholders_data_backup')
        table = 'shareholders_data_backup'
        pk_field = 'en_symbol_12_digit_code, date_m, sh_id'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        print('share_adjusted_data_backup')
        table = 'share_adjusted_data_backup'
        pk_field = 'en_symbol_12_digit_code, date_m, adjusted_type'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        return None


class Sync_AnalyzeData_From_TsetmcRawData(SyncDataBase):

    def __init__(self, source_db_info, destination_db_info, max_packet_size, transaction_mod, sync_period_time):
        SyncDataBase.__init__(self, source_db_info, destination_db_info,
                              max_packet_size, transaction_mod, sync_period_time)
        self.database_name = 'bourse_analyze_data'

    def sync_rule(self, autocommit):

        print('index_data')
        table = 'index_data'
        pk_field = 'en_index_12_digit_code, date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('index_info')
        table = 'index_info'
        pk_field = 'en_index_12_digit_code'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('open_days')
        table = 'open_days'
        pk_field = 'date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('shareholders_data')
        table = 'shareholders_data'
        pk_field = 'en_symbol_12_digit_code, date_m, sh_id'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_adjusted_data')
        table = 'share_adjusted_data'
        pk_field = 'en_symbol_12_digit_code, date_m, adjusted_type'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_daily_data')
        table = 'share_daily_data'
        pk_field = 'en_symbol_12_digit_code, date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_info')
        table = 'share_info'
        pk_field = 'en_symbol_12_digit_code'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_second_data')
        table = 'share_second_data'
        pk_field = 'en_symbol_12_digit_code, date_time'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_status')
        table = 'share_status'
        pk_field = 'en_symbol_12_digit_code, date_m, change_time, change_number'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        return None


class Sync_WebSite_Data_From_TsetmcRawData(SyncDataBase):

    def __init__(self, source_db_info, destination_db_info, max_packet_size, transaction_mod, sync_period_time):
        SyncDataBase.__init__(self, source_db_info, destination_db_info,
                              max_packet_size, transaction_mod, sync_period_time)
        self.database_name = 'bourse_website_data'

    def sync_rule(self, autocommit):

        print('open_days')
        table = 'open_days'
        pk_field = 'date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error


        print('share_info')
        table = 'share_info'
        pk_field = 'en_symbol_12_digit_code'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        if error is not None:
            return error


        return None


class Sync_TsetmcAndAnalyzeData_From_TsetmcRawData(SyncDataBase):

    def __init__(self, source_db_info, destination_db_info, max_packet_size, transaction_mod, sync_period_time):
        SyncDataBase.__init__(self, source_db_info, destination_db_info,
                              max_packet_size, transaction_mod, sync_period_time)
        self.database_name = 'bourse_tsetmc_and_analyze_data'

    def sync_rule(self, autocommit):

        print('client_local_settings')
        table = 'client_local_settings'
        pk_field = 'client_id'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        if error is not None:
            return error


        print('excel_share_daily_data')
        table = 'excel_share_daily_data'
        pk_field = 'en_symbol_12_digit_code, date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('fail_hang_share')
        table = 'fail_hang_share'
        pk_field = 'en_symbol_12_digit_code, date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('fail_integrity_share')
        table = 'fail_integrity_share'
        pk_field = 'en_symbol_12_digit_code, date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('fail_other_share')
        table = 'fail_other_share'
        pk_field = 'en_symbol_12_digit_code, date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('index_data')
        table = 'index_data'
        pk_field = 'en_index_12_digit_code, date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('index_info')
        table = 'index_info'
        pk_field = 'en_index_12_digit_code'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('open_days')
        table = 'open_days'
        pk_field = 'date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('shareholders_data')
        table = 'shareholders_data'
        pk_field = 'en_symbol_12_digit_code, date_m, sh_id'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_adjusted_data')
        table = 'share_adjusted_data'
        pk_field = 'en_symbol_12_digit_code, date_m, adjusted_type'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_daily_data')
        table = 'share_daily_data'
        pk_field = 'en_symbol_12_digit_code, date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_info')
        table = 'share_info'
        pk_field = 'en_symbol_12_digit_code'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_second_data')
        table = 'share_second_data'
        pk_field = 'en_symbol_12_digit_code, date_time'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_status')
        table = 'share_status'
        pk_field = 'en_symbol_12_digit_code, date_m, change_time, change_number'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_adjusted_daily_data')
        table = 'share_adjusted_daily_data'
        pk_field = 'en_symbol_12_digit_code, date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error


        print('share_sub_trad_data')
        table = 'share_sub_trad_data'
        pk_field = 'en_symbol_12_digit_code, date_m, trad_number'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        return None


class Sync_TsetmcAndAnalyzeData_From_AnalyzeData(SyncDataBase):

    def __init__(self, source_db_info, destination_db_info, max_packet_size, transaction_mod, sync_period_time):
        SyncDataBase.__init__(self, source_db_info, destination_db_info,
                              max_packet_size, transaction_mod, sync_period_time)
        self.database_name = 'bourse_tsetmc_and_analyze_data'

    def sync_rule(self, autocommit):

        print('back_test_result')
        table = 'back_test_result'
        pk_field = 'order_id'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        print('index_data')
        table = 'index_data'
        pk_field = 'en_index_12_digit_code, date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('index_info')
        table = 'index_info'
        pk_field = 'en_index_12_digit_code'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('open_days')
        table = 'open_days'
        pk_field = 'date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        print('shareholders_data')
        table = 'shareholders_data'
        pk_field = 'en_symbol_12_digit_code, date_m, sh_id'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        print('share_adjusted_data')
        table = 'share_adjusted_data'
        pk_field = 'en_symbol_12_digit_code, date_m, adjusted_type'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        # print(result)
        # print(error)
        if error is not None:
            return error

        print('share_daily_data')
        table = 'share_daily_data'
        pk_field = 'en_symbol_12_digit_code, date_m'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_info')
        table = 'share_info'
        pk_field = 'en_symbol_12_digit_code'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_second_data')
        table = 'share_second_data'
        pk_field = 'en_symbol_12_digit_code, date_time'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('share_status')
        table = 'share_status'
        pk_field = 'en_symbol_12_digit_code, date_m, change_time, change_number'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('strategy')
        table = 'strategy'
        pk_field = 'user_name, strategy_name'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=True, autocommit_each_table=autocommit)
        if error is not None:
            return error

        print('user')
        table = 'user'
        pk_field = 'username'
        error = self.sync_table(source_table_name=table, destination_table_name=table,
                                pk_field=pk_field, is_multi_pk=False, autocommit_each_table=autocommit)
        if error is not None:
            return error

        return None
