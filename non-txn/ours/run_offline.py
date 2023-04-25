import linecache
import os
import sys
import time

from graphs.offline_graph import OfflineGraph

sys.setrecursionlimit(1000000)


def get_op(op):
    op = op.strip('\n')
    arr = op[2:-1].split(',')
    return {
        'op_type': op[0],
        'var': arr[0],
        'val': arr[1],
        'client_id': int(arr[2]),
        'op_id': int(arr[3]),
    }


def run_offline_graph(file):
    ops = linecache.getlines(file)
    graph = OfflineGraph(4)
    for op in ops:
        op_dict = get_op(op)
        graph.add_node(op_dict['op_type'], op_dict['var'], op_dict['val'], op_dict['client_id'], op_dict['op_id'])
    # graph.detect_bad_pattern_3()
    if graph.detect_bad_pattern_3():
        print('BP3!!!!!')
    # graph.draw_rw()
    # graph.detect_bp2_backward()
    if graph.detect_bp2_backward():
        print('BP2!!!!!!')
    # graph.detect_bp_1_4()
    if graph.detect_bp_1_4():
        print('BP1/4!!!!')


def run_files(url):
    print('Start file list under: ' + url)
    file_list = [fn for fn in os.listdir(url) if fn.endswith('.txt')]
    total_graph_time = 0
    for file in file_list:
        print(file)
        time1 = time.time()
        run_offline_graph(url + file)
        time2 = time.time()
        total_graph_time += time2 - time1
    # print('Avg_time: %.4f' % (total_graph_time / 10))


if __name__ == "__main__":
    urls = [
        # 'data/Galera_dataset/shuffled_Galera/4_client/result_1000/',
        # 'data/Galera_dataset/shuffled_Galera/4_client/result_10000/',
        # 'data/Galera_dataset/shuffled_Galera/4_client/result_50000/',
        # '../DATASET/Galera/4_25_1_15/',
        # '../DATASET/Galera/4_50_1_15/',
        # '../DATASET/Galera/4_75_1_15/',
        # '../DATASET/Galera/4_100_1_15/',
        # '../DATASET/Galera/4_125_1_15/',
        # '../DATASET/Galera/4_150_1_15/'
        './'
    ]
    for url in urls:
        run_files(url)
