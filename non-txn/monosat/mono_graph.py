import copy

from monosat import *

sys.setrecursionlimit(1000000)


class DiGraph:
    def __init__(self):
        self.adj_map = {}

    def add_edge(self, from_node, to_node):
        if from_node in self.adj_map:
            self.adj_map[from_node].add(to_node)
        else:
            self.adj_map[from_node] = {to_node}

    def add_edges(self, from_node, to_nodes):
        if from_node not in self.adj_map:
            self.adj_map[from_node] = set()
        for to_node in to_nodes:
            self.adj_map[from_node].add(to_node)

    def add_vertex(self, new_node):
        if new_node not in self.adj_map:
            self.adj_map[new_node] = set()

    def has_edge(self, from_node, to_node):
        if from_node in self.adj_map and to_node in self.adj_map[from_node]:
            return True
        else:
            return False

    def has_cycle(self):
        for key in list(self.adj_map.keys()):
            reachable = set()
            if self.dfs_util_reach(key, key, reachable):
                return True
        return False

    def dfs_util_reach(self, s, u, reachable):
        if u in self.adj_map:
            for node in self.adj_map[u]:
                if node == s:
                    return True
                elif node in reachable:
                    continue
                else:
                    reachable.add(node)
                    if self.dfs_util_reach(s, node, reachable):
                        return True
        return False

    def dfs_util_all(self, u, reachable):
        if u in self.adj_map:
            for node in self.adj_map[u]:
                if node in reachable:
                    continue
                reachable.add(node)
                self.dfs_util_all(node, reachable)

    def take_closure(self):
        clone_map = self.adj_map.copy()
        for node in self.adj_map:
            reachable = set()
            self.dfs_util_all(node, reachable)
            clone_map[node] = reachable
        self.adj_map = clone_map

    def union_with(self, g):
        for key, value in g.adj_map.items():
            if key in self.adj_map:
                self.adj_map[key] = self.adj_map[key].union(value)
            else:
                self.adj_map[key] = value


class MonosatGraph:
    def __init__(self, ops):
        self.so = DiGraph()
        self.vis = DiGraph()
        self.g = Graph()
        self.wr_rel = {}
        self.nodes = {}
        self.nodes_in_graph = []
        client_in_so = {}
        rs_not_w = {}
        count_id = 0
        for op in ops:
            op = op.strip('\n')
            arr = op[2:-1].split(',')
            op_dict = {
                'op_type': op[0],
                'var': arr[0],
                'val': arr[1],
                'client_id': int(arr[2]),
                'op_id': int(arr[3]),
            }
            self.nodes[count_id] = op_dict
            self.nodes_in_graph.append(self.g.addNode())
            if op_dict['client_id'] in client_in_so:
                self.so.add_edge(client_in_so[op_dict['client_id']], count_id)
                Assert(self.g.addEdge(self.nodes_in_graph[client_in_so[op_dict['client_id']]],
                                      self.nodes_in_graph[count_id]))
            client_in_so[op_dict['client_id']] = count_id

            if op_dict['op_type'] == 'w':
                if op_dict['var'] in self.wr_rel:
                    self.wr_rel[op_dict['var']].add_vertex(count_id)
                else:
                    graph = DiGraph()
                    graph.add_vertex(count_id)
                    self.wr_rel[op_dict['var']] = graph
                if op_dict['var'] in rs_not_w:
                    new_set = rs_not_w[op_dict['var']].copy()
                    for key in rs_not_w[op_dict['var']]:
                        if self.nodes[key]['val'] == op_dict['val']:
                            self.wr_rel[op_dict['var']].add_edge(count_id, key)
                            new_set.remove(key)
                            Assert(self.g.addEdge(self.nodes_in_graph[count_id], self.nodes_in_graph[key]))
                    rs_not_w[op_dict['var']] = new_set
            else:
                has_wr = False
                if op_dict['var'] in self.wr_rel:
                    for key, t_set in self.wr_rel[op_dict['var']].adj_map.items():
                        if self.nodes[key]['val'] == op_dict['val']:
                            t_set.add(count_id)
                            Assert(self.g.addEdge(self.nodes_in_graph[key], self.nodes_in_graph[count_id]))
                            has_wr = True
                            break
                if not has_wr:
                    if op_dict['var'] in rs_not_w:
                        rs_not_w[op_dict['var']].add(count_id)
                    else:
                        rs_not_w[op_dict['var']] = {count_id}
            count_id += 1
        self.vis = copy.deepcopy(self.so)
        self.so.take_closure()

    def get_wr(self):
        wr = DiGraph()
        for key, digraph in self.wr_rel.items():
            wr.union_with(digraph)
        return wr

    def vis_includes(self, g):
        self.vis.union_with(g)

    def vis_is_trans(self):
        self.vis.take_closure()

    def casual_rw(self):
        for x, wr_x in self.wr_rel.items():
            for t1, t3s in wr_x.adj_map.items():
                for t2 in self.vis.adj_map[t1]:
                    if t2 not in t3s and self.nodes[t2]['var'] == x and self.nodes[t2]['op_type'] == 'w':
                        for t3 in t3s:
                            Assert(self.g.addEdge(self.nodes_in_graph[t3], self.nodes_in_graph[t2]))

    def check_read_zero(self):
        for key, t_set in self.vis.adj_map.items():
            if self.nodes[key]['op_type'] == 'w':
                var = self.nodes[key]['var']
                for t in t_set:
                    if self.nodes[t]['var'] == var and self.nodes[t]['val'] == '0':
                        return True
        return False

    def check_cycle(self):
        Assert(self.g.acyclic(True))
        result = Solve()
        return not result
