import os
import linecache


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


def group(path):
    file_list = [fn for fn in os.listdir(path) if fn.endswith('.txt') and not fn.startswith('group')]
    for file in file_list:
        ops = []
        ops += linecache.getlines(path + file)
        ops_per_trans = 10
        with open(path + 'group_' + file, 'w') as f:
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

group('result/')
