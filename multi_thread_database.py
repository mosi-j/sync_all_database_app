import threading

from Log import *
from database import DataBase
import time
from time import sleep

from my_database_info import get_database_info, laptop_local_access, tsetmc_and_analyze_data, server_lan_access, vps1_remote_access
from my_time import get_now_time_second
import random

import string

class Multi_Thread_DataBase:
    def __init__(self, result, db_info, max_thread, offset, lock, log_obj=None):
        try:
            if log_obj is None:
                self.log = Logging()
                self.log.logConfig(account_id=db_info['db_username'], logging_mod=Log_Mod.console_file)
            else:
                self.log = log_obj

            self.log.trace()
            self.db_info = db_info

            self.result = result
            self.max_thread = max_thread
            self.offset = offset
            self.lock = lock

            self.thread_id = 0
            self.name = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5))
            # self.name = '( ' + self.name + ' )'
            # ''.join(random.choices(string.ascii_uppercase + string.digits, k=N))
            #self.result['select_query_status'][0] = 'waiting'
            #self.result['command_query_status'][0] = 'waiting'


        except Exception as e:
            self.log.error('cant create database object: database_info: {}'.format(db_info), str(e))
            return

    def select_process(self, query, args, id):
        db = DataBase(db_info=self.db_info, log_obj=self.log)

        res, err = db.select_query(query=query,args=args, mod=1)
        if err is None:
            self.lock.acquire()
            for item in res:
                self.result['result'].append(item)

            self.result['sum_select_record'][0] += len(res)
            self.lock.release()

        self.result['error'].append(err)
        #print('thread: {0}:{1} result len: {2}'.format(self.name, id, len(self.result['result'])))
        #print('end thread: {0}:{1}'.format(self.name, id))
        print('{}: {}/{}'.format(self.result['transfer_record_count'][0], self.result['sum_select_record'][0], self.result['sum_insert_record'][0]))


    def command_process(self, query, args, id):
        db = DataBase(db_info=self.db_info, log_obj=self.log)
        err = db.command_query(query=query,args=args, write_log=True)

        self.result['error'].append(err)
        if err is None:
            self.result['sum_insert_record'][0] += len(args)

        #print('thread: {0}:{1} error: {2}'.format(self.name, id, err))
        print('{}: {}/{}'.format(self.result['transfer_record_count'][0], self.result['sum_select_record'][0], self.result['sum_insert_record'][0]))

        #print('end thread: {0}:{1}'.format(self.name, id))

    def command_many_process(self, query, args, id):
        db = DataBase(db_info=self.db_info, log_obj=self.log)

        err = db.command_query_many(query=query,args=args, write_log=True)

        self.result['error'].append(err)
        if err is None:
            self.result['sum_insert_record'][0] += len(args)
        # print('thread: {0}:{1} error: {2}'.format(self.name, id, err))
        print('{}: {}/{}'.format(self.result['transfer_record_count'][0], self.result['sum_select_record'][0], self.result['sum_insert_record'][0]))

        #print('end thread: {0}:{1}'.format(self.name, id))

    def get_thread_count(self, thread_name):
        sum = 0
        for t in threading.enumerate():
            t_name = t.getName().split(':')
            if t_name[0] == thread_name:
                sum += 1
        return sum

    def pop_list(self, lis, count):
        res = list()
        try:
            self.lock.acquire()
            for i in range(count):
                res.append(lis.pop())
        finally:
            self.lock.release()
            return res

    def select_query(self, query, args, all_record_count):
        self.reset_result()
        if all_record_count < 1:
            self.result['select_query_status'][0] = 'stopping'
            #self.result['error'].append('all_record_count = 0')
            sleep(0.001)
            return True

        self.result['select_query_status'][0] = 'running'
        self.start_index = 0

        while self.start_index < all_record_count:
            #if threading.active_count() - 1 < self.max_thread :
            if self.get_thread_count(self.name) < self.max_thread :
                self.thread_id += 1
                #print('start thread {}'.format(self.name))

                if self.start_index + self.offset > all_record_count:
                    offset = all_record_count - self.start_index
                else:
                    offset = self.offset

                new_query = query + ' limit {0}, {1}'.format(self.start_index, offset)

                t = threading.Thread(name='{0}:{1}'.format(self.name, self.thread_id), target=self.select_process, args=(new_query, args, self.thread_id))
                #time.sleep(0.5)
                t.start()
                while not t.is_alive():
                    time.sleep(0.1)
                #print('start thread: {}'.format(t.getName()))

                #time.sleep(0.5)

                self.start_index += offset
            else:
                time.sleep(1)

        for t in threading.enumerate():
            if t != threading.main_thread():
                thread_name = t.getName().split(':')
                if thread_name[0] == self.name:
                    t.join()

        self.result['select_query_status'][0] = 'stopping'
        return True

    def command_query_1(self, query, args):
        self.reset_result()
        self.start_index = 0

        while len(args) > 0:
            if self.get_thread_count(self.name) < self.max_thread :
                self.thread_id += 1
                #thread_args = args.pop(self.offset)
                thread_args = self.pop_list(args, self.offset)


                t = threading.Thread(name='{0}:{1}'.format(self.name, self.thread_id), target=self.command_process,
                                     args=(query, thread_args, self.thread_id))
                #time.sleep(0.1)
                t.start()
                while not t.is_alive():
                    time.sleep(0.1)
                print('start thread: {}'.format(t.getName()))
            else:
                time.sleep(1)

        for t in threading.enumerate():
            if t != threading.main_thread():
                thread_name = t.getName().split(':')
                if thread_name[0] == self.name:
                    t.join()

        return True

    def command_query_many(self, query, args):
        self.reset_result()
        self.start_index = 0

        while len(args) > 0:
            if self.get_thread_count(self.name) < self.max_thread :
                self.thread_id += 1
                #thread_args = args.pop(self.offset)
                thread_args = self.pop_list(args, self.offset)


                t = threading.Thread(name='{0}:{1}'.format(self.name, self.thread_id), target=self.command_many_process,
                                     args=(query, thread_args, self.thread_id))
                #time.sleep(0.1)
                t.start()
                while not t.is_alive():
                    time.sleep(0.1)
                print('start thread: {}'.format(t.getName()))
            else:
                time.sleep(1)

        for t in threading.enumerate():
            if t != threading.main_thread():
                thread_name = t.getName().split(':')
                if thread_name[0] == self.name:
                    t.join()

        return True

    def command_query(self, query, args, status_flag=[False]):
        self.reset_result()
        self.result['command_query_status'][0] = 'running'

        self.start_index = 0

        while (len(args) > 0) or (status_flag[0] is True):
            if len(args) > 0:
                # print(1)
                if self.get_thread_count(self.name) < self.max_thread :
                    # print(2)
                    self.thread_id += 1
                    #thread_args = args.pop(self.offset)
                    thread_args = self.pop_list(args, self.offset)

                    if len(thread_args) > 1:
                        t = threading.Thread(name='{0}:{1}'.format(self.name, self.thread_id), target=self.command_many_process,
                                             args=(query, thread_args, self.thread_id))
                    else:
                        t = threading.Thread(name='{0}:{1}'.format(self.name, self.thread_id), target=self.command_process,
                                             args=(query, thread_args, self.thread_id))
                    #time.sleep(0.1)
                    t.start()
                    while not t.is_alive():
                        time.sleep(0.1)
                    #print('start thread: {}'.format(t.getName()))
                else:
                    # print(-2)
                    sleep(1)
            else:
                # print(-1)
                sleep(1)

        for t in threading.enumerate():
            if t != threading.main_thread():
                thread_name = t.getName().split(':')
                if thread_name[0] == self.name:
                    t.join()

        self.result['command_query_status'][0] = 'stopping'

        # print('-----------------------------------------------------')
        return True

    def reset_result(self):
        try:
            a = self.result['result'][0]
        except:
            self.result['result'] = list()


        try:
            a = self.result['error'][0]
        except:
            self.result['error'] = list()

        try:
            a = self.result['select_query_status'][0]
        except:
            self.result['select_query_status'] = ['waiting']

        try:
            a =self.result['command_query_status'][0]
        except:
            self.result['command_query_status'] = ['waiting']


        try:
            a =self.result['sum_select_record'][0]
        except:
            self.result['sum_select_record'] = [0]

        try:
            a = self.result['sum_insert_record'][0]
        except:
            self.result['sum_insert_record'] = [0]


def select_process(result, db_info, max_thread, record_count, offset, lock, query, args):
    db = Multi_Thread_DataBase(result=result, db_info=db_info, max_thread=max_thread, offset=offset, lock=lock)
    res = db.select_query(query=query, args=args, all_record_count=record_count)

    return res

def command_process(result, db_info, max_thread, offset, lock, query, args, status_flag):
    db = Multi_Thread_DataBase(result=result, db_info=db_info, max_thread=max_thread, offset=offset, lock=lock)
    res = db.command_query(query=query, args=args, status_flag=status_flag)
    return res

def transfer_new_record(source_db_info, destination_db_info, source_table_name, destination_table_name, select_max_thread, insert_max_thread, select_max_offset, insert_max_offset, lock, where_str=None):
    start = get_now_time_second()
    transfer_record = dict()
    transfer_record['result'] = list()
    transfer_record['select_query_status'] = list()
    transfer_record['command_query_status'] = list()
    transfer_record['sum_select_record'] = list()
    transfer_record['sum_insert_record'] = list()

    transfer_record_count = 0

    print('get source record count')

    db = DataBase(db_info=source_db_info)
    if where_str is None:
        query = 'select count(*) from {0}'.format(source_table_name)
    else:
        query = 'select count(*) from {0} where {1}'.format(source_table_name, where_str)

    res, err = db.select_query(query=query, args=(), mod=1)
    if err is None:
        transfer_record_count = res[0][0]

    transfer_record['transfer_record_count'] = [transfer_record_count]

    print('transfer_record_count: {}'.format(transfer_record_count))
    # ----------
    # select_query = 'select * from {}'.format(source_table_name)
    if where_str is None:
        select_query = 'select * from {0}'.format(source_table_name)
    else:
        select_query = 'select * from {0} where {1}'.format(source_table_name, where_str)

    args = ()

    select_thread = threading.Thread(name='select_thread', target=select_process, args=(transfer_record, source_db_info, select_max_thread, transfer_record_count, select_max_offset, lock, select_query, args))

    print('start select thread')
    select_thread.start()
    while not select_thread.is_alive():
        sleep(0.1)

    select_thread_name = select_thread.getName()
    #print(select_thread_name)

    insert_status_flag = [True]
    # ----------
    while len(transfer_record['result']) < 1 and transfer_record['select_query_status'][0] != 'stopping':
        sleep(0.1)
    if len(transfer_record['result']) < 1:

        return 0, 0, transfer_record['error'], get_now_time_second() - start

    v = '%s, ' * len(transfer_record['result'][0])
    v = v.strip(', ')
    insert_query = 'insert ignore into {} values({}) '.format(destination_table_name, v)
    args = transfer_record['result']

    insert_thread = threading.Thread(name='insert_thread', target=command_process, args=(transfer_record, destination_db_info, insert_max_thread, insert_max_offset, lock, insert_query, args, insert_status_flag))

    print('start insert thread')
    insert_thread.start()
    while not insert_thread.is_alive():
        sleep(0.1)

    # ----------
    while transfer_record['select_query_status'][0] != 'stopping':
        #print('{}: {}/{}'.format(transfer_record_count, transfer_record['sum_select_record'][0], transfer_record['sum_insert_record'][0]))
        sleep(0.1)
    insert_status_flag[0] = False
    #print('-----------insert_status_flag: {}'.format(insert_status_flag[0]))

    for t in threading.enumerate():
        if t != threading.main_thread():
            #print('--------{}----------'.format(t.getName()))
            t.join()

    # sleep(1)
    new_record_count = transfer_record_count
    total_transfer_record = transfer_record['sum_insert_record'][0]
    error = transfer_record['error']
    run_time = get_now_time_second() - start

    return new_record_count, total_transfer_record, error, run_time


if __name__ == '__main__':
    start = get_now_time_second()

    source_db_info = get_database_info(pc_name=vps1_remote_access, database_name=tsetmc_and_analyze_data)#'tsetmc_raw_data')
    destination_db_info = get_database_info(pc_name=laptop_local_access, database_name=tsetmc_and_analyze_data)
    source_table_name = 'shareholders_data'
    destination_table_name = 'shareholders_data'
    select_max_thread = 5
    insert_max_thread = 1
    select_max_offset = 10000
    insert_max_offset = 1000
    lock = threading.Lock()

    res = transfer_new_record(source_db_info, destination_db_info, source_table_name, destination_table_name, select_max_thread, insert_max_thread, select_max_offset, insert_max_offset, lock)

    print(get_now_time_second() - start)
    print(res)



    old = False
    if old:
        result1 = dict()
        #result2 = dict()
        db_info = get_database_info(pc_name=laptop_local_access, database_name=tsetmc_and_analyze_data)
        max_thread = 5
        offset = 10000

        #db = Multi_Thread_DataBase(result=result, db_info=db_info, max_thread=max_thread, offset=offset)

        query = 'select * from {} '.format('shareholders_data ')
        args = ()
        record_count = 100000

        #res = select_process(result=result, db_info=db_info,max_thread=max_thread,record_count=record_count,offset=offset,query=query, args=args)
        t1 = threading.Thread(name='1', target=select_process, args=(result1, db_info, max_thread, record_count, offset, lock, query, args))

        #t2 = threading.Thread(name='2', target=select_process, args=(result2, db_info, max_thread, record_count, offset, query, args))
        #time.sleep(0.5)
        t1_res = t1.start()
        while not t1.is_alive():
            time.sleep(0.1)


        #t2_res = t2.start()
        #while not t2.is_alive():
        #    time.sleep(0.5)

        for t in threading.enumerate():
            if t != threading.main_thread():
                t.join()


        print(get_now_time_second() - start)

        start = get_now_time_second()

        result2 = dict()
        db_info = get_database_info(pc_name=laptop_local_access, database_name='tsetmc_raw_data')
        max_thread = 10
        offset = 10000
        record_count = 100000

        v = '%s, ' * len(result1['result'][0])
        v = v.strip(', ')
        query = 'insert into {} values({}) '.format('shareholders_data ', v)
        args = result1['result']
        #args = tuple(result1['result'])

        t2 = threading.Thread(name='2', target=command_process, args=(result2, db_info, max_thread, offset, lock, query, args))

        # t2 = threading.Thread(name='2', target=select_process, args=(result2, db_info, max_thread, record_count, offset, query, args))
        #time.sleep(0.5)
        t1_res = t2.start()
        while not t2.is_alive():
            sleep(0.1)

        # t2_res = t2.start()
        # while not t2.is_alive():
        #    time.sleep(0.5)

        for t in threading.enumerate():
            if t != threading.main_thread():
                t.join()
        # res = select_process(result=result, db_info=db_info,max_thread=max_thread,record_count=record_count,offset=offset,query=query, args=args)

    old = False
    if old:
        sum = 0
        sum1 = 0
        common = list()
        while threading.active_count() > 1:
            #print('result1: ' + str(len(result1['result'])))
            #print('result2 :' + str(len(result2['result'])))
            if len(result2['result']) > 0:
                sum1 += 1
                item = result2['result'][0]
                if item in result1['result']:
                    result1['result'].remove(item)
                    result2['result'].remove(item)
                    sum += 1

            #time.sleep(0.5)

        print(t1_res)
        print(sum)
        print(sum1)

       # print(t2_res)
        #res = db.select_query(query=query, args=args, all_record_count=record_count)
        for t in threading.enumerate():
            if t != threading.main_thread():
                t.join()

        print(len(result2['result']))
        print(len(result1['result']))


        for item in result2['result']:
            if item in result1['result']:
                result1['result'].remove(item)
                result2['result'].remove(item)
                #print('result1: ' + str(len(result1['result'])))
                #print('result2 :' + str(len(result2['result'])))
                #time.sleep(0.001)
        #res_2 = set(result2['result'])
        #res_1 = [item for item in res_2 if item not in result1['result']]

        #print(result['error'])
        #print(len(result['result']))
