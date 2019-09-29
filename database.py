import pymysql
from Log import *


class DataBase:
    # con.query('SET GLOBAL connect_timeout=6000')
    def __init__(self, db_info, log_obj=None):
        try:
            if log_obj is None:
                self.log = Logging()
                self.log.logConfig(account_id=db_info['db_username'], logging_mod=Log_Mod.console_file)
            else:
                self.log = log_obj

            self.log.trace()

            self.db_host_name = db_info['db_host_name']
            self.db_username = db_info['db_username']
            self.db_user_password = db_info['db_user_password']
            self.db_name = db_info['db_name']
            self.db_port = db_info['db_port']
        except Exception as e:
            self.log.error('cant create database object: database_info: {}'.format(db_info), str(e))
            return

    def get_connection(self, ):
        try:
            if self.db_port is None:
                con = pymysql.connect(host=self.db_host_name, user=self.db_username,
                                      password=self.db_user_password, db=self.db_name)
            else:
                con = pymysql.connect(host=self.db_host_name, user=self.db_username,
                                      password=self.db_user_password, db=self.db_name, port=self.db_port)
            return con, None
        except Exception as e:
            self.log.error('cant create connection: host_name: {0} database_name: {1}'.format(self.db_host_name, self.db_name), str(e))
            return False, str(e)

    def select_query(self, query, args, mod=0):
        # mod=0 => return cursor
        # mod=1 => retyrn cursor.fetchall()
        self.log.trace()
        if query == '':
            self.log.error('query in empty')
            return None, 'query in empty'

        con = None
        try:
            con, err = self.get_connection()
            if err is not None:
                raise Exception(err)
            db = con.cursor()
            db.execute(query, args)
            con.close()
        except Exception as e:
            self.log.error('except select_query {0}:{1}:'.format(query, args), str(e))
            try:
                if con.open is True:
                    con.close()
            finally:
                return None, 'except select_query: {}'.format(str(e))

        if mod == 0:
            return db, None
        else:
            return db.fetchall(), None

    def select_query_dictionary(self, query, args, mod=0):
        # mod=0 => return cursor
        # mod=1 => retyrn cursor.fetchall()
        self.log.trace()
        if query == '':
            self.log.error('query in empty')
            return None, 'query in empty'

        con = None
        try:
            con, err = self.get_connection()
            if err is not None:
                raise Exception(err)
            db = con.cursor(pymysql.cursors.DictCursor)
            db.execute(query, args)
            con.close()
        except Exception as e:
            self.log.error('except select_query_dictionary', str(e))
            try:
                if con.open is True:
                    con.close()
            finally:
                return None, 'except select_query_dictionary: {}'.format(str(e))

        if mod == 0:
            return db, None
        else:
            return db.fetchall(), None

    def command_query(self, query, args, write_log=True):
        self.log.trace()
        if query == '':
            self.log.error('query in empty')
            return 'query in empty'
        con = None
        try:
            con, err = self.get_connection()
            if err is not None:
                raise Exception(err)

            db = con.cursor()
            db._defer_warnings = True
            db.autocommit = False
            db.execute(query, args)
            # db.executemany(query, args)
            con.commit()
            con.close()
            return None
        except Exception as e:
            if write_log is True:
                print('command_query. error:{0} query:{1}, args:{2}'.format(e, query, args))
                t = 'cant execute command_query and rollback. query:{0}. args:{1}'.format(query, args)
                self.log.error('{0} {1}:{2}:'.format(t, query, args), str(e))
            try:
                if con.open is True:
                    con.rollback()
                    con.close()
            finally:
                return 'cant execute command_query: {}'.format(str(e))

    def command_query_many(self, query, args, write_log=True):
        self.log.trace()
        if query == '':
            self.log.error('query in empty')
            return 'query in empty'
        con = None
        try:
            con, err = self.get_connection()
            if err is not None:
                raise Exception(err)

            db = con.cursor()
            db._defer_warnings = True
            db.autocommit = False
            # db.execute(query, args)
            db.executemany(query, args)
            con.commit()
            con.close()
            return None
        except Exception as e:
            if write_log is True:
                print('command_query_many. error:{0} query:{1}, args:{2}'.format(e, query, args))

                t = 'cant execute command_query_many and rollback. query:{0}'.format(query)
                #if query.find('share_status') > 0:
                #    t = 'cant execute command_query_many and rollback. query:{0}, args:{1}'.format(query, args)

                self.log.error(t, str(e))
            try:
                if con.open is True:
                    con.rollback()
                    con.close()
            finally:
                return 'cant execute command_query_many: {}'.format(str(e))

    # -------------

    def get_record_count(self, table_name, where_str=None):
        if where_str is None:
            query = 'select count(*) from {0}'.format(table_name)
        else:
            query = 'select count(*) from {0} where {1}'.format(table_name, where_str)

        # query = 'select count(*) from {0}'.format(table_name)
        args = ()
        res, err = self.select_query(query, args, 1)
        if err is not None:
            return 0, err

        return int(res[0][0]), None
