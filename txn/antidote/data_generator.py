import linecache

from antidotedb import *
import random
import threading
import os
import time

import numpy as np


def Zipf(a: np.float64, min: np.uint64, max: np.uint64, size=None):
    """
    Generate Zipf-like random variables,
    but in inclusive [min...max] interval
    """
    if min == 0:
        raise ZeroDivisionError("")

    v = np.arange(min, max + 1)  # values to sample
    p = 1.0 / np.power(v, a)  # probabilities
    p /= np.sum(p)  # normalized

    return np.random.choice(v, size=size, replace=True, p=p)


# 每个单独的op
class Operation:
    op_type = True  # true is write
    variable = 0
    value = 0

    def __init__(self, op_type, variable, value):
        self.op_type = op_type
        self.variable = variable
        self.value = value

    def Read(self, variable):
        self.op_type = False
        self.variable = variable
        self.value = 0

    def Write(self, variable, value):
        self.op_type = True
        self.variable = variable
        self.value = value


# 定义一个dict，包含{0: 0, 1: 0, ..., variable_num-1: 0}
def set_initval(counter, variable_num):
    seq = []
    for i in range(0, variable_num):
        seq.append(i)
        i = i + 1
    counter = counter.fromkeys(seq, 0)
    return counter


# 用法：randon_pick([true,false],[0.5,0.5])
def random_pick(some_list, probabilities):
    x = random.uniform(0, 1)
    cumulative_probability = 0.0
    for item, item_probability in zip(some_list, probabilities):
        cumulative_probability += item_probability
        if x < cumulative_probability:
            break
    return item


def uniform_generator(output_path, his, client, trans, ops, var, wr, tra_type):
    # define params
    his_file = his
    client_num = client
    trans_num = trans
    op_num = ops
    variable_num = var
    # ro, wo, rw = tra_type
    all_hist = []
    for h in range(0, his_file):
        doc = open(output_path + "hist_" + str(h) + ".txt", 'w')
        new_hist = []
        counter = {}
        counter = set_initval(counter, variable_num)
        for c in range(0, client_num):
            new_client = []
            for t in range(0, trans_num):
                new_transaction = []
                key_store = []
                for op in range(0, op_num):
                    # 按照比例选and是读或写
                    if wr == 55:
                        op_type = random_pick([True, False], [0.5, 0.5])
                    elif wr == 19:
                        op_type = random_pick([True, False], [0.1, 0.9])
                    elif wr == 91:
                        op_type = random_pick([True, False], [0.9, 0.1])
                    else:
                        print('Wrong input wr!')

                    if (op_type == False):  # READ
                        #   随机选择variable编号
                        variable = random.randint(0, variable_num - 1)
                        new_op = Operation(False, variable, 0)
                    elif (op_type == True):
                        variable = random.randint(0, variable_num - 1)
                        value = counter[variable] + 1
                        counter[variable] = value  # 更新counter内的值
                        new_op = Operation(True, variable, value)
                    else:
                        print("Error in op_type!")
                    # new_op.Display_info()

                    if (new_op.op_type == True):
                        doc.write("write," + str(new_op.variable) + "," + str(new_op.value) + "\n")
                    elif (new_op.op_type == False):
                        doc.write("read," + str(new_op.variable) + "," + str(new_op.value) + "\n")
                    else:
                        print("Error in file Writing!")
        doc.close()
        print(output_path + "hist_" + str(h) + ".txt" + " succeeded.")


def zipf_generator(output_path, his, client, trans, ops, var, wr, tra_type):
    # define params
    his_file = his
    client_num = client
    trans_num = trans
    op_num = ops
    variable_num = var
    all_hist = []
    for h in range(0, his_file):
        doc = open(output_path + "hist_" + str(h) + ".txt", 'w')
        new_hist = []
        counter = {}
        counter = set_initval(counter, variable_num)

        min = np.uint64(1)
        max = np.uint64(var)

        q = Zipf(1, min, max, client_num * trans_num * op_num)
        variable_list = [int(x) - 1 for x in q]
        counter_for_variable = 0

        for c in range(0, client_num):
            for t in range(0, trans_num):
                for op in range(0, op_num):
                    # 按照比例选and是读或写
                    if wr == 55:
                        op_type = random_pick([True, False], [0.5, 0.5])
                    elif wr == 19:
                        op_type = random_pick([True, False], [0.1, 0.9])
                    elif wr == 91:
                        op_type = random_pick([True, False], [0.9, 0.1])
                    else:
                        print('Wrong input wr!')

                    if op_type == False:  # READ
                        #   使用zipf分布选择variable编号
                        variable = variable_list[counter_for_variable]
                        counter_for_variable = counter_for_variable + 1
                        new_op = Operation(False, variable, 0)
                    elif (op_type == True):
                        variable = variable_list[counter_for_variable]
                        counter_for_variable = counter_for_variable + 1
                        value = counter[variable] + 1
                        counter[variable] = value  # 更新counter内的值
                        new_op = Operation(True, variable, value)
                    else:
                        print("Error in op_type!")
                    # new_op.Display_info()

                    if (new_op.op_type == True):
                        doc.write("write," + str(new_op.variable) + "," + str(new_op.value) + "\n")
                    elif (new_op.op_type == False):
                        doc.write("read," + str(new_op.variable) + "," + str(new_op.value) + "\n")
                    else:
                        print("Error in file Writing!")
        doc.close()
        print(output_path + "hist_" + str(h) + ".txt" + " succeeded.")

def generate_clients(hist_file, n_clients, n_trans, n_ops):
    # 按照client-transaction
    # input a single history file, contains n transactions.
    # Output a list of transaction,each transaction is a list of operation
    fo = open(hist_file, "r")
    print("文件名为: ", fo.name)
    list_line = []
    for line in fo.readlines():  # 依次读取每行
        line = line.strip()  # 去掉每行头尾空白,line is a list, contains all ops in hist file
        list_line.append(line)
    # 关闭文件
    fo.close()
    # print(list_line)
    #     need a three dimension list: clients,trans,ops
    start = 0
    end = n_ops
    list_clients = []

    for i in range(0, n_clients):
        temp_trans = []
        for j in range(0, n_trans):
            temp_ops = list_line[start:end]
            temp_trans.append(temp_ops)
            start = start + n_ops
            end = end + n_ops
        list_clients.append(temp_trans)

    return list_clients

def initialize(key_num, clt):
    tx = clt.start_transaction()
    for i in range(key_num + 10):
        key = Key("bucket", str(i), "LWWREG")
        val = bytes(str(1), 'utf-8')
        tx.update_objects(Register.AssignOp(key, val))
    ok = tx.commit()
    if not ok:
        tx.abort()
        print('Abort transaction...')

    # tx = clt.start_transaction()
    # for key in range(key_num):
    #     antidote_key = Key("bucket", str(key), "LWWREG")
    #     value = tx.read_objects(antidote_key)[0].value()
    #
    #     print(str(key) + ': ' + str(value, 'utf-8'))
    # ok = tx.commit()

class MyThread(threading.Thread):
    def __init__(self, func, args, name=''):
        threading.Thread.__init__(self)
        self.name = name
        self.func = func
        self.args = args
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None

def exec_session(list_of_ops, client_id, clt):
    result_ops = []
    n_trans = len(list_of_ops)
    op_num = 0

    for i in range(0, n_trans):
        tx = clt.start_transaction()
        try:
            temp_tx_op = []
            for j in range(0, len(list_of_ops[i])):

                op = str.split(list_of_ops[i][j], ',', 3)
                # create a new variable
                key = str(op[1])
                antidote_key = Key( "bucket", key, "LWWREG")

                if (op[0] == 'write'):
                    val = bytes(str(op[2]), 'utf-8')
                    tx.update_objects( Register.AssignOp( antidote_key, val))
                    res = tx.read_objects(antidote_key)
                    value = res[0].value()
                    single_op = 'w(' + str(key) + ',' + str(value, 'utf-8') + ',' + str(client_id) + ',' + str(i) + ',' + str(
                        op_num) + ')'
                    if str(value, 'utf-8') == '':
                        print(single_op)
                    temp_tx_op.append(single_op)
                    op_num = op_num + 1
                elif (op[0] == 'read'):
                    #                     print('now read, key is '+key+'.\n')
                    value = tx.read_objects(antidote_key)[0].value()
                    single_op = 'r(' + str(key) + ',' + str(value, 'utf-8') + ',' + str(client_id) + ',' + str(i) + ',' + str(
                        op_num) + ')'
                    temp_tx_op.append(single_op)
                    op_num = op_num + 1
                else:
                    print("unknown wrong type op.")

        except Exception as e:
            print(e)
            print('Exception!!!')
            tx.abort()
        else:
            ok = tx.commit()
            if not ok:
                print('Transaction failed!')
                tx.abort()
            else:
                result_ops.append(temp_tx_op)
    return result_ops

def write_result(result_single_history, file):
    '''
        result_single_history is a three dimensional list
        file is the output path
    '''
    f = open(file, "w")
    for n_clients in range(0, len(result_single_history)):
        for n_trans in range(0, len(result_single_history[0])):
            for n_ops in range(0, len(result_single_history[0][0])):
                f.write(result_single_history[n_clients][n_trans][n_ops] + '\n')
    f.close()
    print(file + ' is completed.')

def exec_history(list_of_ops, clt_list, data_id):
    n_clients = len(list_of_ops)
    thread_sessions = []
    results = []

    for i in range(0, n_clients):
        thread = MyThread(exec_session, (list_of_ops[i], i, clt_list[int(i)]))
        thread_sessions.append(thread)

    for i in range(0, n_clients):
        thread_sessions[i].start()
    for i in range(0, n_clients):
        thread_sessions[i].join()

    for i in range(0, n_clients):
        temp_result = thread_sessions[i].get_result()
        # write_result_new(temp_result, 'result/data/' + str(data_id) +'/result_' + str(i) + '.txt')
        results.append(temp_result)

    return results

def get_op(op):
    op = op.strip('\n')
    arr = op[2:-1].split(',')
    return {
        'op_type': op[0],
        'var': arr[0],
        'val': arr[1],
        'client_id': int(arr[2]),
        'tra_id': int(arr[3]),
    }

def format_data(path, ops_per_trans):
    file_list = [fn for fn in os.listdir(path) if fn.endswith('.txt') and not fn.startswith('data')]
    for file in file_list:
        ops = []
        ops += linecache.getlines(path + file)
        with open(path + 'data_' + file, 'w') as f:
            now_id = 0
            cnt = ops_per_trans
            for op in ops:
                op_dict = get_op(op)
                cnt = cnt - 1
                op_dict['tra_id'] = now_id
                if cnt == 0:
                    cnt = ops_per_trans
                    now_id += 1
                f.write(str(op_dict['op_type']) + '(' + str(op_dict['var']) + ',' + str(op_dict['val']) + ',' + str(
                    op_dict['client_id']) + ',' + str(op_dict['tra_id']) + ')\n')
        os.remove(path + file)

if __name__ == '__main__':
    client_num = 3
    transaction_num = 500
    ops_per_trans = 2
    wr_profile = 55
    key_num = 100
    path = ''
    generate_num = 20
    clt1 = AntidoteClient('127.0.0.1', 8101)
    clt2 = AntidoteClient('127.0.0.1', 8201)
    clt3 = AntidoteClient('127.0.0.1', 8301)
    # initialize(key_num, clt1)
    # time.sleep(10)
    # tx = clt3.start_transaction()
    # for i in range(key_num):
    #     key = Key("bucket", str(i), "LWWREG")
    #     value = tx.read_objects(key)[0].value()
    #     print(i)
    #     print(str(value, 'utf-8'))
    # ok = tx.commit()
    # if not ok:
    #     tx.abort()
    #     print('Abort transaction...')

    uniform_generator(path + 'hist/', generate_num, client_num, transaction_num, ops_per_trans, key_num, wr_profile, '')
    for i in range(15, generate_num):
        op_list = generate_clients(path + 'hist/hist_' + str(i) + '.txt', client_num, transaction_num, ops_per_trans)
        initialize(key_num, clt1)
        result_file = 'result/result_' + str(i) + '.txt'
        time.sleep(10)
        result_single_history = exec_history(op_list, [clt1, clt2, clt3], i)
        write_result(result_single_history, result_file)
    format_data('result/', ops_per_trans)
