class OfflineTxnGraph:
    # constructor
    def __init__(self, client_num):
        self.nodes = []
        self.transactions = {}
        self.set_w = {}
        self.set_r_without_wr = {}
        self.set_r_with_wr = {}
        self.forward_edge = {}
        self.backward_edge = {}
        self.read_zero = {}
        self.client_newly_added = {}
        self.vector = {}
        self.wr_set = {}
        self.client_num = client_num
        self.transaction_to_client = {}

    # Manually input last node in one transaction
    def add_node(self, op_type, var, val, client_id, tra_id, is_last_in_tra):
        node_id = len(self.nodes)
        self.nodes.append(
            {'type': op_type, 'var': var, 'val': val, 'client_id': client_id, 'tra_id': tra_id})
        if tra_id in self.transactions:
            self.transactions[tra_id].append(node_id)
        else:
            self.transactions[tra_id] = [node_id]
        if tra_id not in self.forward_edge:
            self.forward_edge[tra_id] = set()
        if tra_id not in self.backward_edge:
            self.backward_edge[tra_id] = set()
        if client_id in self.client_newly_added:
            last_tra_id = self.client_newly_added[client_id]
            if tra_id != last_tra_id:
                if tra_id not in self.forward_edge[last_tra_id]:
                    self.forward_edge[last_tra_id].add(tra_id)
                    self.backward_edge[tra_id].add(last_tra_id)
        # read operation
        if op_type == 'r':
            has_wr = False
            if var in self.set_w:
                for node_index in self.set_w[var]:
                    if self.nodes[node_index]['val'] == val:
                        has_wr = True
                        new_txn = self.nodes[node_index]['tra_id']
                        if new_txn != tra_id:
                            self.forward_edge[new_txn].add(tra_id)
                            self.backward_edge[tra_id].add(new_txn)
                        if node_index in self.wr_set:
                            self.wr_set[node_index].append(node_id)
                        else:
                            self.wr_set[node_index] = [node_id]
                        break
            if has_wr:
                if var in self.set_r_with_wr:
                    self.set_r_with_wr[var].add(node_id)
                else:
                    self.set_r_with_wr[var] = {node_id}
            else:
                if var in self.set_r_without_wr:
                    self.set_r_without_wr[var].add(node_id)
                else:
                    self.set_r_without_wr[var] = {node_id}
            if val == '0':
                if tra_id not in self.read_zero:
                    self.read_zero[tra_id] = set()
                self.read_zero[tra_id].add(var)
        # write operation
        else:
            if var in self.set_w:
                self.set_w[var].append(node_id)
            else:
                self.set_w[var] = [node_id]
            if var in self.set_r_without_wr:
                s = self.set_r_without_wr[var].copy()
                for node_index in self.set_r_without_wr[var]:
                    if self.nodes[node_index]['val'] == val:
                        new_txn = self.nodes[node_index]['tra_id']
                        if new_txn != tra_id:
                            self.forward_edge[tra_id].add(new_txn)
                            self.backward_edge[new_txn].add(tra_id)
                        if node_id in self.wr_set:
                            self.wr_set[node_id].append(node_index)
                        else:
                            self.wr_set[node_id] = [node_index]
                        s.remove(node_index)
                        if var in self.set_r_with_wr:
                            self.set_r_with_wr[var].add(node_index)
                        else:
                            self.set_r_with_wr[var] = {node_index}
                self.set_r_without_wr[var] = s
        if is_last_in_tra:
            self.transaction_to_client[tra_id] = client_id
            is_visited = set()
            self.vector[tra_id] = self.set_vector(tra_id, client_id)
            stack = [tra_id]
            while len(stack):
                tra_index = stack.pop()
                for key in self.forward_edge[tra_index]:
                    if key in is_visited:
                        continue
                    for key1 in self.backward_edge[key]:
                        for i in range(self.client_num):
                            if i != self.transaction_to_client[key]:
                                self.vector[key][i] = max(self.vector[key1][i], self.vector[key][i])
                    stack.append(key)
                is_visited.add(tra_index)
        self.client_newly_added[client_id] = tra_id

    def detect_bp2_backward(self):
        for txn_id, vars in self.read_zero.items():
            is_visited = set()
            stack = [txn_id]
            while len(stack):
                txn_index = stack.pop()
                is_visited.add(txn_index)
                for key in self.backward_edge[txn_index]:
                    if key in is_visited:
                        continue
                    for node_id in self.transactions[key]:
                        if self.nodes[node_id]['type'] == 'w' and self.nodes[node_id]['var'] in vars:
                            return True
                    stack.append(key)
        return False

    def detect_bad_pattern_3(self):
        has_not_read = False
        for key, values in self.set_r_without_wr.items():
            if len(values):
                for node_id in values:
                    if self.nodes[node_id]['val'] != '0':
                        has_not_read = True
                        break
                if has_not_read:
                    break
        return has_not_read

    def detect_bp_1_4(self):
        for var, read_nodes in self.set_r_with_wr.items():
            for read_node in read_nodes:
                write_node = None
                for node_id in self.set_w[var]:
                    if node_id in self.wr_set and read_node in self.wr_set[node_id]:
                        write_node = node_id
                        break
                if write_node == None:
                    continue
                else:
                    write_txn = self.nodes[write_node]['tra_id']
                    read_txn = self.nodes[read_node]['tra_id']
                    if write_txn == read_txn:
                        continue
                    for w_node in self.set_w[var]:
                        new_write_txn = self.nodes[w_node]['tra_id']
                        if w_node == write_node or write_txn == new_write_txn or read_txn == new_write_txn:
                            continue
                        elif self.compare_larger(write_txn, new_write_txn) and self.compare_larger(new_write_txn, read_txn):
                            # BP4
                            return True
                    if self.compare_larger(read_txn, write_txn):
                        # BP1
                        return True

    def dfs(self, node, end, enter_list, result_list):
        if node in enter_list:
            return False
        if node == end:
            return True
        enter_list.append(node)
        node_list = self.forward_edge[node]
        for n in node_list:
            if self.dfs(n, end, enter_list, result_list):
                result_list.append(n)
                return True
        return False




    def compare_larger(self, pre_id, next_id):
        for i in range(self.client_num):
            if self.vector[next_id][i] < self.vector[pre_id][i]:
                return False
        return True

    def set_vector(self, tra_id, client_id):
        vec = [0] * self.client_num
        for key in self.backward_edge[tra_id]:
            for i in range(self.client_num):
                if i == client_id and self.transaction_to_client[key] == client_id:
                    vec[i] = max(vec[i], self.vector[key][i] + 1)
                elif i != client_id:
                    vec[i] = max(vec[i], self.vector[key][i])
        if vec[client_id] == 0:
            vec[client_id] = 1
        return vec
