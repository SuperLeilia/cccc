import linecache
import os
import sys

from monosat_graph.mono_txn_graph import MonosatTxnGraph

sys.setrecursionlimit(1000000)

# Causal checking
def run_monosat_txn_graph_causal(file):
    raw_ops = linecache.getlines(file)
    index = 0
    for i in range(len(raw_ops)):
        if raw_ops[index] == '\n':
            index = i
            break
    if index != 0:
        raw_ops = raw_ops[0:index]
    causal_hist = MonosatTxnGraph(raw_ops)
    wr = causal_hist.get_wr()
    causal_hist.vis_includes(wr)
    causal_hist.vis_is_trans()
    causal_hist.casual_ww()
    if causal_hist.check_cycle():
        print('Find Violation!')

# Read atomic checking
def run_monosat_txn_graph_ra(file):
    raw_ops = linecache.getlines(file)
    index = 0
    for i in range(len(raw_ops)):
        if raw_ops[i] == '\n':
            index = i
            break
    if index != 0:
        raw_ops = raw_ops[0:index]
    causal_hist = MonosatTxnGraph(raw_ops)
    wr = causal_hist.get_wr()
    causal_hist.vis_includes(wr)
    causal_hist.casual_ww()
    if causal_hist.check_cycle():
        print('Find Violation!')


if __name__ == "__main__":
    file_list = [fn for fn in os.listdir('test_ra/') if 'Store' not in fn and fn.startswith('data')]
    for file in file_list:
        path = 'test_ra/' + file
        print(path)
        # Read atomic checking
        run_monosat_txn_graph_ra(path)
        # Causal checking
        # run_monosat_txn_graph_causal(path)
