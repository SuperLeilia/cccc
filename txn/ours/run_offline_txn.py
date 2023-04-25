import linecache
import os
import sys
import time

from graphs.offline_txn_graph import OfflineTxnGraph

sys.setrecursionlimit(1000000)


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


def run_offline_txn_graph(file):
    ops = linecache.getlines(file)
    graph = OfflineTxnGraph(6)
    for i in range(len(ops)):
        op_dict = get_op(ops[i])
        graph.add_node(op_dict['op_type'], op_dict['var'], op_dict['val'], op_dict['client_id'], op_dict['tra_id'],
                       True if i == len(ops) - 1 or get_op(ops[i + 1])['tra_id'] != op_dict['tra_id'] else False)
    if graph.detect_bp_1_4():
        print('BP1/4!!!!')

def run_files(url):
    file_list = [fn for fn in os.listdir('../yugabyte/result/') if 'Store' not in fn and fn.startswith('data')]
    for file in file_list:
        path = '../yugabyte/result/' + file
        print(path)
        run_offline_txn_graph(path)


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


if __name__ == "__main__":
    urls = [
        'data/Galera_dataset/shuffled_Galera/4_client/result_1000/',
    ]
    for url in urls:
        run_files(url)
