import time

import psycopg2
import os
import linecache
import json
import random
import threading


# single op
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

# Drop All - discard all data and start from a clean slate.
def drop_and_create():
    conn = psycopg2.connect("host=127.0.0.1 port=5433 dbname=test user=yugabyte password=yugabyte")

    conn.set_session(autocommit=False)
    cur = conn.cursor()

    cur.execute(
        """
        DROP TABLE IF EXISTS test
        """)

    cur.execute(
        """
        CREATE TABLE test (key varchar, value varchar,  primary key (key))
        """)

    conn.commit()
    cur.close()
    conn.close()


def generate_clients(hist_file, n_clients, n_trans, n_ops):
    # input a single history file, contains n transactions.
    # Output a list of transaction,each transaction is a list of operation
    fo = open(hist_file, "r")
    print("File name: ", fo.name)
    list_line = []
    for line in fo.readlines():  # read a line
        line = line.strip()  # line is a list, contains all ops in hist file
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


def create_variables(n_variable):
    '''
    Initialize, set all value of keys to 0
    '''
    conn = psycopg2.connect("host=127.0.0.1 port=5433 dbname=test user=yugabyte password=yugabyte")

    conn.set_session(autocommit=False)
    cur = conn.cursor()

    for i in range(0, n_variable):
        key = str(i)
        # Create data.
        cur.execute("INSERT INTO test (key, value) VALUES (%s, %s)",
                    (key, '0'))
    conn.commit()
    cur.close()
    conn.close()


def op_query(cur, key):
    cur.execute("SELECT value FROM test WHERE key='" + key + "'")
    row = cur.fetchone()
    return row[0]


def op_update(cur, key, val):
    cur.execute("update test set value=%s where key=%s",
                (val, key))


'''
    return 3-dimension array result_clients
'''


def exec_history(list_of_ops):
    n_clients = len(list_of_ops)
    n_trans = len(list_of_ops[0])
    n_ops = len(list_of_ops[0][0])
    thread_clients = []
    clients = []
    result_clients = []
    # for i in range(0, n_clients):
    #     conn = psycopg2.connect("host=127.0.0.1 port=5433 dbname=test user=yugabyte password=yugabyte")
    #     clients.append(conn)
    for i in range(0, n_clients):
        new_client = MyThread(exe, (list_of_ops[i], i))
        thread_clients.append(new_client)
    for i in range(0, n_clients):
        thread_clients[i].start()
    for i in range(0, n_clients):
        thread_clients[i].join()
    for i in range(0, n_clients):
        temp_result = thread_clients[i].get_result()
        result_clients.append(temp_result)
    return result_clients


def exe(list_of_ops, client_id):
    result_ops = []
    n_trans = len(list_of_ops)
    trans_num = int(n_trans / 10)
    op_num = 0
    count = 0
    client_conn = psycopg2.connect("host=127.0.0.1 port=5433 dbname=test user=yugabyte password=yugabyte")
    client_conn.set_session(autocommit=False)

    while True:
        if count == trans_num:
            break
        try:
            with client_conn:
                with client_conn.cursor() as cur:
                    temp_tx_op = []
                    for j in range(0, len(list_of_ops[count])):
                        #             print('now dealing with : '+list_of_ops[i][j])
                        op = str.split(list_of_ops[count][j], ',', 3)
                        # create a new variable
                        key = str(op[1])
                        if (op[0] == 'write'):
                            val = str(op[2])
                            # print('now write, key is '+key+', value is '+val+'.\n')
                            op_update(cur, key, val)
                            single_op = 'w(' + str(key) + ',' + str(val) + ',' + str(client_id) + ',' + str(
                                count) + ',' + str(
                                op_num) + ')'
                            temp_tx_op.append(single_op)
                            op_num = op_num + 1
                        elif (op[0] == 'read'):
                            # print('now read, key is '+key+'.\n')
                            value = op_query(cur, key)
                            single_op = 'r(' + str(key) + ',' + str(value) + ',' + str(client_id) + ',' + str(
                                count) + ',' + str(
                                op_num) + ')'
                            temp_tx_op.append(single_op)
                            op_num = op_num + 1
                        else:
                            print("unknown wrong type op.")
                client_conn.commit()
        except Exception as e:
            client_conn.rollback()
            sleep_ms = 0.1 * (random.random() + 0.5)
            time.sleep(sleep_ms)
            continue
        result_ops.append(temp_tx_op)
        count += 1

    client_conn.close()

    return result_ops


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


def main():
    client_num = 5
    transaction_num = 20
    wr_profile = 55
    key_num = 120
    ops_per_trans = 20
    file_num = 10

    # generate raw data
    uniform_generator('hist/', file_num, client_num, transaction_num * 10, ops_per_trans, key_num, wr_profile)
    for i in range(0, file_num):
        drop_and_create()
        generate_file = 'hist/hist_' + str(i) + '.txt'
        result_file = 'result/result_' + str(i) + '.txt'

        create_variables(key_num)
        list_of_ops = generate_clients(generate_file, client_num, transaction_num*10, ops_per_trans)
        result_single_history = exec_history(list_of_ops)
        write_result(result_single_history, result_file)
    format_data('result/', ops_per_trans)



if __name__ == '__main__':
    main()
    print('DONE!')
