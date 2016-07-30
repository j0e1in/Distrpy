from redis import connection

SYM_STAR = b'*'
SYM_DOLLAR = b'$'
SYM_CRLF = b'\r\n'
SYM_EMPTY = b''


class Talker(connection.Connection):
    def __init__(self, host, port):
        super().__init__(host=host, port=port)

    def _recv_multi(self, n):
        resp = []
        while len(resp) < n:
            resp.append(self.read_response().decode('utf-8'))
        return resp

    def talk_raw(self, command, recv=None):
        recv = recv or self.read_response
        for c in command:
            self.send_packed_command(c)
        return recv().decode('utf-8')

    def talk(self, *args):
        self.send_command(*args)
        r = self.read_response()
        if isinstance(r, list):
            for i in r:
                i.decode('utf-8')
            return r
        else:
            return r.decode('utf-8')

    def talk_bulk(self, cmd_list):
        return self.talk_raw(self.pack_commands(cmd_list),
                             recv=lambda: self._recv_multi(len(cmd_list)))

    def close(self):
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, except_type, except_obj, tb):
        self.close()
        return False

c = connection.Connection()
CMD_INFO = [c.pack_command('info')]
CMD_CLUSTER_NODES = [c.pack_command('cluster', 'nodes')]
CMD_CLUSTER_INFO = [c.pack_command('cluster', 'info')]


class ClusterNode(object):
    def __init__(self, node_id, latest_know_ip_address_and_port,
                 role_in_cluster, node_id_of_master_if_it_is_a_slave,
                 last_ping_sent_time, last_pong_received_time, node_index,
                 link_status, *assigned_slots):
        self.node_id = node_id
        host, port = latest_know_ip_address_and_port.split(':')
        self.host = host
        self.port = int(port)
        self.role_in_cluster = (role_in_cluster.split(',')[1]
                                if 'myself' in role_in_cluster
                                else role_in_cluster)
        self.master_id = node_id_of_master_if_it_is_a_slave
        self.assigned_slots = []
        self.slots_migrating = False
        for slots_range in assigned_slots:
            if '[' == slots_range[0] and ']' == slots_range[-1]:
                # exclude migrating slot
                self.slots_migrating = True
                continue
            if '-' in slots_range:
                begin, end = slots_range.split('-')
                self.assigned_slots.extend(list(range(int(begin), int(end) + 1)))
            else:
                self.assigned_slots.append(int(slots_range))

        self._talker = None

    def talker(self):
        if self._talker is None:
            self._talker = Talker(self.host, self.port)
        return self._talker

    def close(self):
        if self._talker is not None:
            self._talker.close()
            self._talker = None


class BaseBalancer(object):
    def weight(self, clusternode):
        return 1


def base_balance_plan(nodes, balancer=None):
    if balancer is None:
        balancer = BaseBalancer()
    nodes = [n for n in nodes if 'master' == n.role_in_cluster]
    origin_slots = [len(n.assigned_slots) for n in nodes]
    total_slots = sum(origin_slots)
    weights = [balancer.weight(n) for n in nodes]
    total_weight = sum(weights)

    result_slots = [int(total_slots * w / total_weight) for w in weights]
    frag_slots = int(total_slots - sum(result_slots))

    migratings = [[n, r - o] for n, r, o in
                  zip(nodes, result_slots, origin_slots)]

    for m in migratings:
        if frag_slots > -m[1] > 0:
            frag_slots += m[1]
            m[1] = 0
        elif frag_slots <= -m[1]:
            m[1] += frag_slots
            break

    migrating = sorted([m for m in migratings if m[1] != 0],
                       key=lambda x: x[1])
    mig_out = 0
    mig_in = len(migrating) - 1

    plan = []
    while mig_out < mig_in:
        if migrating[mig_in][1] < -migrating[mig_out][1]:
            plan.append((migrating[mig_out][0], migrating[mig_in][0],
                         migrating[mig_in][1]))
            migrating[mig_out][1] += migrating[mig_in][1]
            mig_in -= 1
        elif migrating[mig_in][1] > -migrating[mig_out][1]:
            plan.append((migrating[mig_out][0], migrating[mig_in][0],
                         -migrating[mig_out][1]))
            migrating[mig_in][1] += migrating[mig_out][1]
            mig_out += 1
        else:
            plan.append((migrating[mig_out][0], migrating[mig_in][0],
                         migrating[mig_in][1]))
            mig_out += 1
            mig_in -= 1

    return plan
