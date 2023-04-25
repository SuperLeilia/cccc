class OfflineGraph:
    # constructor
    def __init__(self, client_num):
        self.nodes = []
        self.set_w = {}
        self.set_r_without_wr = {}
        self.set_r_with_wr = {}
        self.forward_edge = []
        self.backward_edge = []
        self.read_zero = {}
        self.process_newly_added = {}
        self.vector = []
        self.wr_set = {}
        self.client_num = client_num

    def detect_bp2_backward(self):
        for var, read_id in self.read_zero.items():
            is_visited = [False] * len(self.nodes)
            stack = [read_id]
            while len(stack):
                node_index = stack.pop()
                if is_visited[node_index]:
                    continue
                is_visited[node_index] = True
                for key in list(self.backward_edge[node_index].keys()):
                    if is_visited[key]:
                        continue
                    node = self.nodes[key]
                    if node['type'] == 'w' and node['var'] == var:
                        return True
                    stack.append(key)
        return False

    def detect_bp2_w(self):
        for var, read_id in self.read_zero.items():
            if var in self.set_w:
                for write_id in self.set_w[var]:
                    if self.compare_larger(write_id, read_id):
                        return True
        return False

    def add_node(self, op_type, var, val, client_id, op_id):
        has_out_edge = False
        node_id = len(self.nodes)
        self.nodes.append({'type': op_type, 'var': var, 'val': val, 'client_id': client_id, 'op_id': op_id})
        self.forward_edge.append({})
        self.backward_edge.append({})
        # read operation
        if op_type == 'r':
            is_po_wr = False
            num_in_client = 1
            has_wr = False
            if client_id in self.process_newly_added:
                last_node_id = self.process_newly_added[client_id]
                num_in_client = self.vector[last_node_id][client_id] + 1
                if self.nodes[last_node_id]['type'] == 'w' and self.nodes[last_node_id]['var'] == var and \
                        self.nodes[last_node_id]['val'] == val:
                    is_po_wr = True
                    self.forward_edge[last_node_id][node_id] = 'wr'
                    self.backward_edge[node_id][last_node_id] = 'wr'
                    has_wr = True
                    if last_node_id in self.wr_set:
                        self.wr_set[last_node_id].append(node_id)
                    else:
                        self.wr_set[last_node_id] = [node_id]
                else:
                    self.forward_edge[last_node_id][node_id] = 'po'
                    self.backward_edge[node_id][last_node_id] = 'po'
            vec = self.set_vector(node_id, client_id, num_in_client)
            self.vector.append(vec)
            if not is_po_wr and var in self.set_w:
                has_new_wr = False
                for node_index in self.set_w[var]:
                    if self.nodes[node_index]['val'] == val:
                        has_wr = True
                        if not self.compare_larger(node_index, node_id):
                            has_new_wr = True
                            self.forward_edge[node_index][node_id] = 'wr'
                            self.backward_edge[node_id][node_index] = 'wr'
                        if node_index in self.wr_set:
                            self.wr_set[node_index].append(node_id)
                        else:
                            self.wr_set[node_index] = [node_id]
                        break
                if has_new_wr:
                    vec = self.set_vector(node_id, client_id, num_in_client)
                    self.vector[node_id] = vec
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
                self.read_zero[var] = node_id
        # write operation
        else:
            if var in self.set_w:
                self.set_w[var].append(node_id)
            else:
                self.set_w[var] = [node_id]
            vec = [0] * self.client_num

            if client_id in self.process_newly_added:
                last_node_id = self.process_newly_added[client_id]
                self.forward_edge[last_node_id][node_id] = 'po'
                self.backward_edge[node_id][last_node_id] = 'po'
                for i in range(self.client_num):
                    if i == client_id:
                        vec[i] = self.vector[last_node_id][i] + 1
                    else:
                        vec[i] = self.vector[last_node_id][i]
            else:
                vec[client_id] = 1
            self.vector.append(vec)

            if var in self.set_r_without_wr:
                s = self.set_r_without_wr[var].copy()
                for node_index in self.set_r_without_wr[var]:
                    if self.nodes[node_index]['val'] == val:
                        has_out_edge = True
                        self.forward_edge[node_id][node_index] = 'wr'
                        self.backward_edge[node_index][node_id] = 'wr'
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
            is_visited = [False] * len(self.nodes)
            if has_out_edge:
                stack = [node_id]
                while len(stack):
                    node_index = stack.pop()
                    for key in list(self.forward_edge[node_index].keys()):
                        if is_visited[key]:
                            continue
                        for key1, value1 in self.backward_edge[key].items():
                            for i in range(self.client_num):
                                if i != self.nodes[key]['client_id']:
                                    self.vector[key][i] = max(self.vector[key1][i], self.vector[key][i])
                        stack.append(key)
                    is_visited[node_index] = True
        self.process_newly_added[client_id] = node_id

    def dfs_graph(self):
        for node_id in range(len(self.nodes)):
            is_visited = [False] * len(self.nodes)
            stack = [node_id]
            while len(stack):
                node_index = stack.pop()
                for key in list(self.forward_edge[node_index].keys()):
                    if key == node_id:
                        return True
                    if is_visited[key]:
                        continue
                    stack.append(key)
                is_visited[node_index] = True
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

    def draw_rw(self):
        for var, w_keys in self.set_w.items():
            for i in range(len(w_keys) - 1):
                for j in range(i + 1, len(w_keys)):
                    if w_keys[i] in self.wr_set and self.compare_larger(w_keys[i], w_keys[j]):
                        read_nodes = self.wr_set[w_keys[i]]
                        for read_node in read_nodes:
                            if not self.compare_larger(read_node, w_keys[j]):
                                self.forward_edge[read_node][w_keys[j]] = 'rw'
                                self.backward_edge[w_keys[j]][read_node] = 'rw'
                    elif w_keys[j] in self.wr_set and self.compare_larger(w_keys[j], w_keys[i]):
                        read_nodes = self.wr_set[w_keys[j]]
                        for read_node in read_nodes:
                            if not self.compare_larger(read_node, w_keys[i]):
                                self.forward_edge[read_node][w_keys[i]] = 'rw'
                                self.backward_edge[w_keys[i]][read_node] = 'rw'

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
                    for w_node in self.set_w[var]:
                        if w_node == write_node:
                            continue
                        elif self.compare_larger(write_node, w_node) and self.compare_larger(w_node, read_node):
                            # BP4
                            return True
                    if self.compare_larger(read_node, write_node):
                        #BP1
                        return True

    def compare_larger(self, pre_id, next_id):
        for i in range(self.client_num):
            if self.vector[next_id][i] < self.vector[pre_id][i]:
                return False
        return True

    def set_vector(self, node_id, client_id, num_in_client):
        vec = [0] * self.client_num
        vec[client_id] = num_in_client
        for key, value in self.backward_edge[node_id].items():
            for i in range(self.client_num):
                if i == client_id:
                    continue
                else:
                    vec[i] = max(vec[i], self.vector[key][i])
        return vec
