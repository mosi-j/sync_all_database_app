from database import DataBase
from my_time import get_now_time_string, get_now_time_second


class SyncDataBase:
    def __init__(self, source_db_info, destination_db_info, max_packet_size, commit_mod, sync_period_time):
        self.source_db_info = source_db_info
        self.destination_db_info = destination_db_info
        self.max_packet_size = max_packet_size
        self.commit_mod = commit_mod
        self.sync_period_time = sync_period_time

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
        print('start sync database: {}'.format(self.database_name))
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

    def update_table(self, source_table_name, destination_table_name, pk_field, commit_mod, destination_connection=None, where_str=None):
        # max_transfer_record = 40
        destination_con = None
        new_record_count = 0
        total_transfer_record = 0

        pk_count = pk_field.split()
        pk_count = len(pk_count)


        if commit_mod == 'no_commit':
            if destination_connection is None:
                error = 'connection error'
                return new_record_count, total_transfer_record, error
            else:
                destination_con = destination_connection

        #source_db = DataBase(db_info=source_db_info)
        #destination_db = DataBase(db_info=destination_db_info)

        # --------------
        if where_str is None:
            query = 'select {0} from {1}'.format(pk_field, destination_table_name)
        else:
            query = 'select {0} from {1} where {2}'.format(pk_field, destination_table_name, where_str)

        args = ()

        destination_pk_list, err = self.destination_db.select_query(query, args, 1)
        if err is not None:
            return new_record_count, total_transfer_record, err

        if pk_count == 1:
            l = list()
            for item in destination_pk_list:
                l.append(item[0])
            destination_pk_list = tuple(l)

        # --------------
        if len(destination_pk_list) > 0:  # destination have record
            if where_str is None:
                query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_pk_list)
            else:
                query = 'select {0} from {1} where {3} and ({0}) not in {2}'.format(pk_field, source_table_name, destination_pk_list, where_str)

            if len(destination_pk_list) == 1:
                query = query[:-2] + ')'

                #destination_list = destination_pk_list[0]
                #query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_list)
            #else:
                #query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_pk_list)

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
            if where_str is None:
                query = 'select count(*) from {0}'.format(source_table_name)
            else:
                query = 'select count(*) from {0} where {1}'.format(source_table_name, where_str)
            args = ()
            new_record_count, err = self.source_db.select_query(query, args, 1)
            if err is not None:
                return new_record_count, total_transfer_record, err

            new_record_count = int(new_record_count[0][0])

        # ------------
        if new_record_count == 0:  # no new record
            return new_record_count, total_transfer_record, None

        if commit_mod == 'all_commit':
            destination_con, err = self.destination_db.get_connection()

        start_index = 0

        while True:
            if start_index >= new_record_count:
                break

            if commit_mod == 'batch_commit':
                destination_con, err = self.destination_db.get_connection()

            # get new record from source
            if len(destination_pk_list) > 0:  # destination have record
                new_record = new_record_pk_list[start_index:start_index + self.max_packet_size]
                if where_str is None:
                    query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
                else:
                    query = 'select * from {0} where {3} and ({1}) in {2}'.format(source_table_name, pk_field, new_record, where_str)

                if len(new_record) == 1:
                    #query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
                    query = query[:-2] + ')'
               # else:
                    #query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
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
            query = 'insert into {0} values ({1})'.format(destination_table_name, v)
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

    def update_table0(self, source_table_name, destination_table_name, pk_field, commit_mod, destination_connection=None):
        # max_transfer_record = 40
        destination_con = None
        new_record_count = 0
        total_transfer_record = 0

        pk_count = pk_field.split()
        pk_count = len(pk_count)


        if commit_mod == 'no_commit':
            if destination_connection is None:
                error = 'connection error'
                return new_record_count, total_transfer_record, error
            else:
                destination_con = destination_connection

        #source_db = DataBase(db_info=source_db_info)
        #destination_db = DataBase(db_info=destination_db_info)

        # --------------
        query = 'select {0} from {1}'.format(pk_field, destination_table_name)
        args = ()

        destination_pk_list, err = self.destination_db.select_query(query, args, 1)
        if err is not None:
            return new_record_count, total_transfer_record, err

        if pk_count == 1:
            l = list()
            for item in destination_pk_list:
                l.append(item[0])
            destination_pk_list = tuple(l)

        # --------------
        if len(destination_pk_list) > 0:  # destination have record
            query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_pk_list)

            if len(destination_pk_list) == 1:
                query = query[:-2] + ')'

                #destination_list = destination_pk_list[0]
                #query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_list)
            #else:
                #query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_pk_list)

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
            query = 'select count(*) from {0}'.format(source_table_name)
            args = ()
            new_record_count, err = self.source_db.select_query(query, args, 1)
            if err is not None:
                return new_record_count, total_transfer_record, err

            new_record_count = int(new_record_count[0][0])

        # ------------
        if new_record_count == 0:  # no new record
            return new_record_count, total_transfer_record, None

        if commit_mod == 'all_commit':
            destination_con, err = self.destination_db.get_connection()

        start_index = 0

        while True:
            if start_index >= new_record_count:
                break

            if commit_mod == 'batch_commit':
                destination_con, err = self.destination_db.get_connection()

            # get new record from source
            if len(destination_pk_list) > 0:  # destination have record
                new_record = new_record_pk_list[start_index:start_index + self.max_packet_size]
                query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)

                if len(new_record) == 1:
                    #query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
                    query = query[:-2] + ')'
               # else:
                    #query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
            else:  # destination have not record
                query = 'select * from {0} limit {1}, {2}'.format(source_table_name, start_index, self.max_packet_size)

            args = ()
            new_record, err = self.source_db.select_query(query, args, 1)
            if err is not None:
                return new_record_count, total_transfer_record, err

            v = '%s, ' * len(new_record[0])
            v = v.strip(', ')
            # transfer to destination
            query = 'insert into {0} values ({1})'.format(destination_table_name, v)
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
        return 0, 0, None

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
            print(table[0]['name'])
            new_record_count, total_transfer_record, error = self.update_table(source_table_name=table[0]['name'],
                                                                               destination_table_name=table[1]['name'],
                                                                               pk_field=table[1]['pk_field'],
                                                                               commit_mod=commit_mod,
                                                                               destination_connection=destination_connection,
                                                                               where_str=table[2])
            if error is not None:
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


class Sync_WebSite_Data_From_TsetmcRawData(SyncDataBase):

    def __init__(self, source_db_info, destination_db_info, max_packet_size, commit_mod, sync_period_time):
        SyncDataBase.__init__(self, source_db_info, destination_db_info,
                              max_packet_size, commit_mod, sync_period_time)
        self.database_name = 'bourse_website_data'

    def run_sync_rule(self):
        from my_database_info import open_days, index_data, share_info

        rule_table = [[open_days, open_days, None],
                     [index_data, index_data, "en_index_12_digit_code <> 'IRX6X01T0007'"],
                     [share_info, share_info, None]]

        return self.sync_rule(rule_table=rule_table)

    def sync_rule0(self):
        from my_database_info import open_days, index_data, share_info
        destination_connection = None
        all_new_record_count = 0
        all_total_transfer_record = 0
        error = None

        rul_table = [[open_days, open_days, None],
                     [index_data, index_data, "en_index_12_digit_code <> 'IRX6X01T0007'"],
                     [share_info, share_info, None]]
        # rul_table = [[share_info,share_info]]

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

        for table in rul_table:
            # --------------------
            print(table[0]['name'])
            new_record_count, total_transfer_record, error = self.update_table(source_table_name=table[0]['name'],
                                                                               destination_table_name=table[1]['name'],
                                                                               pk_field=table[1]['pk_field'],
                                                                               commit_mod=commit_mod,
                                                                               destination_connection=destination_connection,
                                                                               where_str=table[2])
            if error is not None:
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




class SyncDataBase1:
    def __init__(self, source_db_info, destination_db_info, max_packet_size, commit_mod, sync_period_time, rule_table):
        self.source_db_info = source_db_info
        self.destination_db_info = destination_db_info
        self.max_packet_size = max_packet_size
        self.commit_mod = commit_mod
        self.sync_period_time = sync_period_time
        self.rule_table = rule_table

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
        print('start sync database: {}'.format(self.database_name))
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

    def update_table(self, source_table_name, destination_table_name, pk_field, commit_mod, destination_connection=None, where_str=None):
        # max_transfer_record = 40
        destination_con = None
        new_record_count = 0
        total_transfer_record = 0

        pk_count = pk_field.split()
        pk_count = len(pk_count)


        if commit_mod == 'no_commit':
            if destination_connection is None:
                error = 'connection error'
                return new_record_count, total_transfer_record, error
            else:
                destination_con = destination_connection

        #source_db = DataBase(db_info=source_db_info)
        #destination_db = DataBase(db_info=destination_db_info)

        # --------------
        if where_str is None:
            query = 'select {0} from {1}'.format(pk_field, destination_table_name)
        else:
            query = 'select {0} from {1} where {2}'.format(pk_field, destination_table_name, where_str)

        args = ()

        destination_pk_list, err = self.destination_db.select_query(query, args, 1)
        if err is not None:
            return new_record_count, total_transfer_record, err

        if pk_count == 1:
            l = list()
            for item in destination_pk_list:
                l.append(item[0])
            destination_pk_list = tuple(l)

        # --------------
        if len(destination_pk_list) > 0:  # destination have record
            if where_str is None:
                query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_pk_list)
            else:
                query = 'select {0} from {1} where {3} and ({0}) not in {2}'.format(pk_field, source_table_name, destination_pk_list, where_str)

            if len(destination_pk_list) == 1:
                query = query[:-2] + ')'

                #destination_list = destination_pk_list[0]
                #query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_list)
            #else:
                #query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_pk_list)

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
            if where_str is None:
                query = 'select count(*) from {0}'.format(source_table_name)
            else:
                query = 'select count(*) from {0} where {1}'.format(source_table_name, where_str)
            args = ()
            new_record_count, err = self.source_db.select_query(query, args, 1)
            if err is not None:
                return new_record_count, total_transfer_record, err

            new_record_count = int(new_record_count[0][0])

        # ------------
        if new_record_count == 0:  # no new record
            return new_record_count, total_transfer_record, None

        if commit_mod == 'all_commit':
            destination_con, err = self.destination_db.get_connection()

        start_index = 0

        while True:
            if start_index >= new_record_count:
                break

            if commit_mod == 'batch_commit':
                destination_con, err = self.destination_db.get_connection()

            # get new record from source
            if len(destination_pk_list) > 0:  # destination have record
                new_record = new_record_pk_list[start_index:start_index + self.max_packet_size]
                if where_str is None:
                    query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
                else:
                    query = 'select * from {0} where {3} and ({1}) in {2}'.format(source_table_name, pk_field, new_record, where_str)

                if len(new_record) == 1:
                    #query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
                    query = query[:-2] + ')'
               # else:
                    #query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
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
            query = 'insert into {0} values ({1})'.format(destination_table_name, v)
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

    def update_table0(self, source_table_name, destination_table_name, pk_field, commit_mod, destination_connection=None):
        # max_transfer_record = 40
        destination_con = None
        new_record_count = 0
        total_transfer_record = 0

        pk_count = pk_field.split()
        pk_count = len(pk_count)


        if commit_mod == 'no_commit':
            if destination_connection is None:
                error = 'connection error'
                return new_record_count, total_transfer_record, error
            else:
                destination_con = destination_connection

        #source_db = DataBase(db_info=source_db_info)
        #destination_db = DataBase(db_info=destination_db_info)

        # --------------
        query = 'select {0} from {1}'.format(pk_field, destination_table_name)
        args = ()

        destination_pk_list, err = self.destination_db.select_query(query, args, 1)
        if err is not None:
            return new_record_count, total_transfer_record, err

        if pk_count == 1:
            l = list()
            for item in destination_pk_list:
                l.append(item[0])
            destination_pk_list = tuple(l)

        # --------------
        if len(destination_pk_list) > 0:  # destination have record
            query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_pk_list)

            if len(destination_pk_list) == 1:
                query = query[:-2] + ')'

                #destination_list = destination_pk_list[0]
                #query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_list)
            #else:
                #query = 'select {0} from {1} where ({0}) not in {2}'.format(pk_field, source_table_name, destination_pk_list)

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
            query = 'select count(*) from {0}'.format(source_table_name)
            args = ()
            new_record_count, err = self.source_db.select_query(query, args, 1)
            if err is not None:
                return new_record_count, total_transfer_record, err

            new_record_count = int(new_record_count[0][0])

        # ------------
        if new_record_count == 0:  # no new record
            return new_record_count, total_transfer_record, None

        if commit_mod == 'all_commit':
            destination_con, err = self.destination_db.get_connection()

        start_index = 0

        while True:
            if start_index >= new_record_count:
                break

            if commit_mod == 'batch_commit':
                destination_con, err = self.destination_db.get_connection()

            # get new record from source
            if len(destination_pk_list) > 0:  # destination have record
                new_record = new_record_pk_list[start_index:start_index + self.max_packet_size]
                query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)

                if len(new_record) == 1:
                    #query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
                    query = query[:-2] + ')'
               # else:
                    #query = 'select * from {0} where ({1}) in {2}'.format(source_table_name, pk_field, new_record)
            else:  # destination have not record
                query = 'select * from {0} limit {1}, {2}'.format(source_table_name, start_index, self.max_packet_size)

            args = ()
            new_record, err = self.source_db.select_query(query, args, 1)
            if err is not None:
                return new_record_count, total_transfer_record, err

            v = '%s, ' * len(new_record[0])
            v = v.strip(', ')
            # transfer to destination
            query = 'insert into {0} values ({1})'.format(destination_table_name, v)
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
            print(table[0]['name'])
            new_record_count, total_transfer_record, error = self.update_table(source_table_name=table[0]['name'],
                                                                               destination_table_name=table[1]['name'],
                                                                               pk_field=table[1]['pk_field'],
                                                                               commit_mod=commit_mod,
                                                                               destination_connection=destination_connection,
                                                                               where_str=table[2])
            if error is not None:
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

    sync = SyncDataBase1(source_db_info1, destination_db_info1, max_packet_size, commit_mod1, sync_period_time, rule_table)

    #sync = Sync_WebSite_Data_From_TsetmcRawData(source_db_info1, destination_db_info1, max_packet_size, commit_mod1, sync_period_time)

    print(sync.run(force=True))
