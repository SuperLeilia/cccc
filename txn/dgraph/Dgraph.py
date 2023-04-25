import datetime
import json
import pydgraph

import linecache
import os
import time
import random
import matplotlib.pyplot as plt
import pandas as pd
import sys
import threading
import time

import numpy as np
from matplotlib import pyplot as plt


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

    def Display_info(self):
        if (op_type == True):
            print("write," + str(variable) + "," + str(value))
        elif (op_type == False):
            print("read," + str(variable) + "," + str(value))
        else:
            print("Error in Operation op_type!")


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


class MyThread(threading.Thread):
    def __init__(self, func, args):
        threading.Thread.__init__(self)
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


def uniform_generator(output_path, his, client, trans, ops, var, wr):
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
        for c in range(0, client_num):
            new_client = []
            for t in range(0, trans_num):
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
                        while variable in key_store:
                            variable = random.randint(0, variable_num - 1)
                        key_store.append(variable)
                        new_op = Operation(False, variable, 0)
                    elif (op_type == True):
                        variable = random.randint(0, variable_num - 1)
                        while variable in key_store:
                            variable = random.randint(0, variable_num - 1)
                        key_store.append(variable)
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


# def zipf_generator(output_path, his, client, trans, ops, var, wr):
#
#     #define params
#     his_file = his
#     client_num = client
#     trans_num = trans
#     op_num = ops
#     variable_num = var
#     all_hist = []
#     for h in range (0,his_file):
#         doc =open(output_path+"hist_"+str(h)+".txt",'w')
#         new_hist = []
#         counter = {}
#         counter = set_initval(counter,variable_num)
#
#         min = np.uint64(1)
#         max = np.uint64(var)
#
#         q = Zipf(1, min, max, client_num*trans_num*op_num)
#         variable_list = [int(x)-1 for x in q]
#         counter_for_variable = 0
#
#         for c in range (0,client_num):
#             new_client = []
#             for t in range (0,trans_num):
#                 new_transaction = []
#                 for op in range (0,op_num):
#                     #按照比例选and是读或写
#                     if wr == 55:
#                         op_type = random_pick([True,False],[0.5,0.5])
#                     elif wr == 19:
#                         op_type = random_pick([True,False],[0.1,0.9])
#                     elif wr ==91:
#                         op_type = random_pick([True,False],[0.9,0.1])
#                     else:
#                         print('Wrong input wr!')
#
#                     if(op_type==False): #READ
#                     #   使用zipf分布选择variable编号
#                         variable = variable_list[counter_for_variable]
#                         counter_for_variable = counter_for_variable+1
#                         new_op = Operation(False,variable,0)
#                     elif(op_type==True):
#                         variable = variable_list[counter_for_variable]
#                         counter_for_variable = counter_for_variable+1
#                         value = counter[variable] +1
#                         counter[variable] = value #更新counter内的值
#                         new_op = Operation(True,variable,value)
#                     else:
#                         print("Error in op_type!")
#                     #new_op.Display_info()
#
#                     if(new_op.op_type==True):
#                         doc.write("write," + str(new_op.variable) + "," + str(new_op.value)+"\n")
#                     elif(new_op.op_type==False):
#                         doc.write("read," + str(new_op.variable) + "," + str(new_op.value)+"\n")
#                     else:
#                         print("Error in file Writing!")
#         doc.close()
#         print(output_path+"hist_"+str(h)+".txt"+" succeeded.")


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


def create_client_stub():
    return pydgraph.DgraphClientStub('localhost:9080')


# Create a client.
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


# Drop All - discard all data and start from a clean slate.
def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))


def set_schema(client):
    schema = """
    key: string @index(exact) .
    value: int .

    type Operation {
        key
        value

    }
    """
    return client.alter(pydgraph.Operation(schema=schema))


def create_data(client, key, value):
    # Create a new transaction.
    txn = client.txn()
    try:
        # Create data.
        p = {
            #             'uid': '_:' + key,
            'dgraph.type': 'Operation',
            'key': key,
            'value': value
        }

        # Run mutation.
        response = txn.mutate(set_obj=p)

        # Commit transaction.
        txn.commit()


    finally:
        # Clean up. Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


def query(client, key):
    query = """query all($k: string) {
            data(func: eq(key, $k)) {
                key
                value
            }
        }"""
    variables = {'$k': key}
    response = client.txn(read_only=True).query(query, variables=variables)
    rs = json.loads(response.json)
    value = rs['value']
    return value


#     print(json.dumps(rs, ensure_ascii=False, indent=2))

#     # Print results.
#     print('key value : {}'.format(ppl['all']))


def query_all(client):
    query = """{
            data(func: type(Operation)) {
                key
                value
            }
        }"""
    response = client.txn(read_only=True).query(query)
    rs = json.loads(response.json)
    print(json.dumps(rs, ensure_ascii=False, indent=2))


def update(client, key, value):
    txn = client.txn()

    variables = {'$k': key}
    query = """
  {
    u as var(func: eq(key, "2"))
  }
    """
    #     response = client.txn(read_only=True).query(query, variables=variables)

    #     rs = json.loads(response.json)
    #     print('query status: '+json.dumps(rs, ensure_ascii=False, indent=2))

    cond = "@if(eq(len(u), 1))"
    nquads = """
          uid(u) <value> "{0}" .
    """.format(str(value))
    mutation = txn.create_mutation(cond=cond, set_nquads=nquads)
    request = txn.create_request(mutations=[mutation], query=query, commit_now=True)
    txn.do_request(request)


#     txn.commit()


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


def create_variables(client_stub, n_variable):
    client = create_client(client_stub)
    set_schema(client)
    for i in range(0, n_variable):
        txn = client.txn()
        try:
            key = str(i)
            # Create data.
            p = {
                #             'uid': '_:' + key,
                'dgraph.type': 'Operation',
                'key': key,
                'value': 0
            }

            # Run mutation.
            response = txn.mutate(set_obj=p)

            # Commit transaction.
            txn.commit()
        except Exception as e:
            print('Error: {}'.format(e))
            txn.discard()
        finally:
            txn.discard()


# finally:
# Clean up. Calling this after txn.commit() is a no-op and hence safe.
# txn.discard()


def op_query(client, key):
    query = """query all($k: string) {
            data(func: eq(key, $k)) {
                key
                value
            }
        }"""
    variables = {'$k': key}
    response = client.txn(read_only=True).query(query, variables=variables)
    rs = json.loads(response.json)
    result = rs['data']
    #     result is a list, and result[0] contains a dic of key and value
    #     print(result[0]['value']) #result is value

    return result[0]['value']


def op_update(txn, key, val):
    variables = {'$k': key}
    query = """
  {{
  u as var(func: eq(key, "{k}"))
  }}
  """.format(k=str(key))
    cond = "@if(eq(len(u), 1))"
    nquads = """
          uid(u) <value> "{0}" .
    """.format(str(val))
    mutation = txn.create_mutation(cond=cond, set_nquads=nquads)
    request = txn.create_request(mutations=[mutation], query=query, commit_now=False)
    txn.do_request(request)


'''
    返回的result_clients是三位结果数组
'''


def exec_history(list_of_ops, client_stub):
    n_clients = len(list_of_ops)
    n_trans = len(list_of_ops[0])
    n_ops = len(list_of_ops[0][0])
    thread_clients = []
    clients = []
    result_clients = []
    for i in range(0, n_clients):
        clients.append(create_client(client_stub))
    for i in range(0, n_clients):
        new_client = MyThread(exe, (list_of_ops[i], clients[i], i))
        thread_clients.append(new_client)
    for i in range(0, n_clients):
        thread_clients[i].start()
    for i in range(0, n_clients):
        thread_clients[i].join()
    for i in range(0, n_clients):
        temp_result = thread_clients[i].get_result()
        result_clients.append(temp_result)
    return result_clients


'''
    这里的list_of_ops是二维数组，trans，ops
    client 是实例
    clientid是第i个，用于写入文件
    返回值result_ops也是二维数组
'''


def exe(list_of_ops, client, client_id):
    result_ops = []
    n_trans = len(list_of_ops)
    trans_num = int(n_trans / 10)
    op_num = 0
    count = 0

    while True:
        if count == trans_num:
            break
        txn = client.txn()
        temp_tx_op = []
        try:
            for j in range(0, len(list_of_ops[count])):
                #             print('now dealing with : '+list_of_ops[i][j])
                op = str.split(list_of_ops[count][j], ',', 3)
                # create a new variable
                key = str(op[1])

                if (op[0] == 'write'):
                    val = str(op[2])
                    # print('now write, key is '+key+', value is '+val+'.\n')
                    op_update(txn, key, val)
                    # value = op_query(client, key)
                    single_op = 'w(' + str(key) + ',' + str(val) + ',' + str(client_id) + ',' + str(count) + ',' + str(
                        op_num) + ')'
                    temp_tx_op.append(single_op)
                    op_num = op_num + 1
                elif (op[0] == 'read'):
                    # print('now read, key is '+key+'.\n')
                    value = op_query(client, key)
                    single_op = 'r(' + str(key) + ',' + str(value) + ',' + str(client_id) + ',' + str(count) + ',' + str(
                        op_num) + ')'
                    temp_tx_op.append(single_op)
                    op_num = op_num + 1
                else:
                    print("unknown wrong type op.")
            txn.commit()
            result_ops.append(temp_tx_op)
            count += 1
        except Exception as e:
            # print('Error: {}'.format(e))
            txn.discard()
        finally:
            txn.discard()

    return result_ops


'''
    uniform
'''


def main():
    client_num = [2]
    transaction_num = [10]
    wr_profile = [55]
    key_num = [20]
    ops_per_trans = 10

    for c in client_num:
        for t in transaction_num:
            for wr in wr_profile:
                for k in key_num:
                    # path = '../DATA_Dgrpah/client' + str(c) + '/transaction' + str(t) + '/wr' + str(wr) + '/key' +str(k) +'/uniform/'

                    uniform_generator('hist/', 10, c, t*10, ops_per_trans, k, wr)
                    for i in range(0, 10):
                        client_stub = create_client_stub()
                        drop_all(create_client(client_stub))
                        generate_file = 'hist/hist_' + str(i) + '.txt'
                        result_file = 'result/result_' + str(i) + '.txt'

                        create_variables(client_stub, k)
                        list_of_ops = generate_clients(generate_file, c, t*10, ops_per_trans)
                        result_single_history = exec_history(list_of_ops, client_stub)
                        write_result(result_single_history, result_file)

                        drop_all(create_client(client_stub))
                        client_stub.close()


if __name__ == '__main__':
    # try:
    main()
    print('DONE!')
    # except Exception as e:
    #     print('Error: {}'.format(e))

'''
    Zipf
'''

# def main():
#     client_num = [5,10,20,30]
#     transaction_num = [100]
#     wr_profile = [55]
#     key_num = [1000]

#     for c in client_num:
#         for t in transaction_num:
#             for wr in wr_profile:
#                 for k in key_num:
#                     path = '../DATA_Dgrpah/client' + str(c) + '/transaction' + str(t) + '/wr' + str(wr) + '/key' +str(k) +'/zipf/'

#                     uniform_generator(path,10,c,t,1,k,wr)
#                     for i in range(0,10):
#                         client_stub = create_client_stub()
#                         generate_file = path + 'hist_'+str(i)+'.txt'
#                         result_file = path + 'result_'+str(i)+'.txt'

#                         create_variables(client_stub,1,k)
#                         list_of_ops = generate_clients(generate_file,c,t,1)
#                         result_single_history = exec_history(list_of_ops, client_stub)
#                         write_result(result_single_history,result_file)

#                         drop_all(create_client(client_stub))
#                         client_stub.close()


# if __name__ == '__main__':
#     try:
#         main()
#         print('DONE!')
#     except Exception as e:
#         print('Error: {}'.format(e))


# client_stub = create_client_stub()
# client = create_client(client_stub)
# drop_all(client)
