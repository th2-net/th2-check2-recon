import collections
import logging
import queue

import comparator
from th2 import infra_pb2

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)


class QueueListener:

    def __init__(self, routing_key: str, cache_size: int, channel, timeout, index: int) -> None:
        self.queue = channel.queue_declare(queue='',
                                           exclusive=True)
        self.queue_name = self.queue.method.queue
        self.routing_key = routing_key
        self.blocking_queue = queue.Queue(cache_size)
        self.buffer = collections.deque()
        self.timeout = timeout
        self.index = index

    def peek_element(self) -> infra_pb2.Message:
        if len(self.buffer) == 0:
            head = self.blocking_queue.get(block=True, timeout=self.timeout)
            self.buffer.append(head)
        return self.buffer[-1]

    def remove_head(self):
        self.buffer.pop()


def log_table_messages(messages, col_size):
    result = ''
    field_names = []
    for message in messages:
        for field_name in message.fields.keys():
            field_names.append(field_name)

    result += '\n|' + "message_type".ljust(col_size, ' ') + '|'
    for message in messages:
        result += message.metadata.message_type.ljust(col_size, ' ') + '|'

    result += '\n|' + "id".ljust(col_size, ' ') + '|'
    for message in messages:
        result += str(message.metadata.id.sequence).ljust(col_size, ' ') + '|'

    result += '\n|' + "timestamp".ljust(col_size, ' ') + '|'
    for message in messages:
        result += str(message.metadata.timestamp.seconds).ljust(col_size, ' ') + '|'

    result += '\n' + '-' * ((len(messages) + 1) * col_size + len(messages) + 2)
    for field_name in field_names:
        result += '\n|' + field_name.ljust(col_size, ' ') + '|'
        for message in messages:
            value: infra_pb2.Value = message.fields[field_name]
            if len(value.simple_value) > 0:
                field_value = value.simple_value
            else:
                if len(value.list_value.values) > 0:
                    field_value = "NOT_EMPTY_LIST"  # TODO fix
                else:
                    if len(value.message_value.fields) > 0:
                        field_value = "SUB_MESSAGE"  # TODO fix
                    else:
                        field_value = "EMPTY"
            result += field_value.ljust(col_size, ' ') + '|'
    return result


def log_result(indices, cache, queue_listeners, parent_id):
    col_size = 30
    result = ""
    for elem_idx in range(len(indices)):

        result += '\n\n' + '*' * ((len(queue_listeners.values()) + 1) * col_size + len(queue_listeners.values()) + 2)
        result += '\n|' + "Field".ljust(col_size, ' ') + '|'
        for i in range(len(queue_listeners.values())):
            key = ""
            for queue_listener in queue_listeners.values():
                if i == queue_listener.index:
                    key = queue_listener.routing_key
                    break
            result += key.ljust(col_size, ' ') + '|'
        result += '\n' + '*' * ((len(queue_listeners.values()) + 1) * col_size + len(queue_listeners.values()) + 2)

        messages = []
        for seq_idx in range(len(indices[elem_idx])):
            messages.append(cache[seq_idx][indices[elem_idx][seq_idx]])
        result += log_table_messages(messages, col_size)
    logging.info(result)


def recon(queue_listeners: dict, cache_size: int, time_interval, report_id, message_comparator):
    logging.info("Recon is started")
    cache = Cache(len(queue_listeners.values()), cache_size, time_interval)
    while True:
        if not cache.fill_cache(queue_listeners):
            if cache.probable_min_time != -1:
                cache.filter_cache_by_time(queue_listeners, report_id)
            continue
        indices = Recon.calc(cache.data, message_comparator)
        log_result(indices, cache.data, queue_listeners, report_id)
        if len(indices) != 0:
            cache.filter_cache_by_indices(indices)
        else:
            cache.filter_cache_by_time(queue_listeners, report_id)


class Cache:

    def __init__(self, seq_count: int, max_size, time_interval) -> None:
        self.data = [[] for x in range(seq_count)]
        self.max_size = max_size
        self.time_interval = time_interval
        self.min_time = -1
        self.probable_min_time = -1

    def fill_sub_cache(self, queue_listener: QueueListener):
        for x in range(self.max_size):
            try:
                head = queue_listener.peek_element()
                head_time = head.metadata.timestamp.seconds
                if len(self.data[queue_listener.index]) >= self.max_size:
                    self.update_probable_min_time(head_time)
                    return

                if self.min_time == -1 or head_time < self.min_time:
                    self.min_time = head_time

                if self.min_time <= head_time <= self.min_time + self.time_interval:
                    self.data[queue_listener.index].append(head)
                    queue_listener.remove_head()
                else:
                    self.update_probable_min_time(head_time)
                    break

            except queue.Empty:
                logging.info("%r: no messages in %r sec" % (queue_listener.routing_key, queue_listener.timeout))
                break

    def fill_cache(self, queue_listeners: dict):
        is_ok = True
        for queue_listener in queue_listeners.values():
            self.fill_sub_cache(queue_listener)
            if len(self.data[queue_listener.index]) == 0:
                logging.info('%r: cache is empty from %rs to %rs' % (
                    queue_listener.routing_key, str(self.min_time), str(self.min_time + self.time_interval)))
                is_ok = False
        return is_ok

    def filter_cache_by_time(self, queue_listeners: dict, parent_id):
        self.min_time = self.probable_min_time
        max_time_of_del = -1
        for seq_idx in range(len(self.data)):
            while len(self.data[seq_idx]) > 0 and self.data[seq_idx][-1].metadata.timestamp.seconds > self.min_time:
                routing_key = ""
                for queue_listener in queue_listeners.values():
                    if queue_listener.index == seq_idx:
                        routing_key = queue_listener.routing_key
                        break
                logging.info(
                    "%r: Out of time interval %r" % (routing_key, self.data[seq_idx][-1].metadata.message_type))
                if self.data[seq_idx][-1].metadata.timestamp.seconds > max_time_of_del:
                    max_time_of_del = self.data[seq_idx][-1].metadata.timestamp.seconds
                del self.data[seq_idx][-1]
        self.update_min_time(max_time_of_del)

    def filter_cache_by_indices(self, indices: list):
        max_time_of_del = -1
        for seq_idx in range(len(self.data)):
            for elem_idx in range(len(indices)):
                if self.data[seq_idx][indices[elem_idx][seq_idx] - elem_idx].metadata.timestamp.seconds \
                        > max_time_of_del:
                    max_time_of_del = self.data[seq_idx][
                        indices[elem_idx][seq_idx] - elem_idx].metadata.timestamp.seconds
                del self.data[seq_idx][indices[elem_idx][seq_idx] - elem_idx]
        self.update_min_time(max_time_of_del)

    def update_min_time(self, max_time_of_del):
        for i in range(len(self.data)):
            if len(self.data[i]) > 0:
                self.min_time = self.data[i][0].metadata.timestamp.seconds
        if self.min_time > max_time_of_del != -1:
            self.min_time = max_time_of_del
        for seq_idx in range(len(self.data)):
            for elem_idx in range(len(self.data[seq_idx])):
                if self.min_time == -1 or self.data[seq_idx][elem_idx].metadata.timestamp.seconds < self.min_time:
                    self.min_time = self.data[seq_idx][elem_idx].metadata.timestamp.seconds

    def update_probable_min_time(self, head_time):
        if self.probable_min_time == -1:
            self.probable_min_time = head_time - self.time_interval
        else:
            if head_time - self.time_interval < self.probable_min_time:
                self.probable_min_time = head_time - self.time_interval


class Recon:
    data = {}
    prev = {}

    @classmethod
    def calc(cls, seq, message_comparator):
        indices = []
        matched = [False] * len(seq[1])
        for i in range(len(seq[0])):
            for j in range(len(seq[1])):
                if not matched[j] and message_comparator.match(seq[0][i], seq[1][j]):
                    matched[j] = True
                    indices.append([i, j])
                    message_comparator.check(seq[0][i], seq[1][j])
                    break
        return indices

    @classmethod
    def find_lcs(cls, seq):
        indices = [0] * len(seq)
        lengths = [len(seq[i]) + 1 for i in range(len(seq))]

        while 1:
            need_continue = False
            for index in indices:
                if index == 0:
                    cls.__set_lcs(0, indices)
                    need_continue = True
            if not need_continue:
                is_all_equals = True
                for i in range(len(indices) - 1):
                    if not comparator.equals(seq[i][indices[i] - 1], seq[i + 1][indices[i + 1] - 1]):
                        is_all_equals = False
                        break
                if is_all_equals:
                    prev_indices = [i - 1 for i in indices]
                    cls.__set_lcs(cls.__get_lcs(prev_indices) + 1, indices)
                    cls.__set_prev(prev_indices, indices)
                else:
                    max_lcs = -1
                    max_index = -1
                    for i in range(len(indices)):
                        indices[i] -= 1
                        cur_lcs = cls.__get_lcs(indices)
                        if cur_lcs > max_lcs:
                            max_lcs = cur_lcs
                            max_index = i
                        indices[i] += 1
                    max_indices = [x for x in indices]
                    max_indices[max_index] -= 1
                    cls.__set_lcs(max_lcs, indices)
                    cls.__set_prev(max_indices, indices)

            if not cls.__increment_indices(indices, lengths):
                break
        result_str = cls.__get_lcs_indices([i - 1 for i in lengths])
        arr_indices = []
        for cur_str in result_str:
            cur_arr = []
            for i in range(0, len(cur_str) - 2, 3):
                cur_arr.append(int(cur_str[i:i + 3]))
            arr_indices.append(cur_arr)
        return arr_indices

    @classmethod
    def __get_lcs_indices(cls, indices):
        result = []
        for index in indices:
            if index == 0:
                return result

        prev_indices = [index - 1 for index in indices]
        if cls.__get_prev(indices) == cls.__from_indices(prev_indices):
            result.extend(cls.__get_lcs_indices(prev_indices))
            result.append(cls.__from_indices(prev_indices))
        else:
            prev_indices[:] = [index + 1 for index in prev_indices]
            for i in range(len(indices)):
                prev_indices[i] -= 1
                if cls.__get_prev(indices) == cls.__from_indices(prev_indices):
                    result.extend(cls.__get_lcs_indices(prev_indices))
                prev_indices[i] += 1
        return result

    @classmethod
    def __increment_indices(cls, indices, lengths):
        pos = len(indices) - 1
        indices[pos] += 1
        while pos >= 0 and indices[pos] == lengths[pos]:
            indices[pos] = 0
            pos -= 1
            indices[pos] += 1
        return pos >= 0

    @classmethod
    def __from_indices(cls, indices):
        result = ''
        for index in indices:
            result += f'{index:03}'
        return result

    @classmethod
    def __get_lcs(cls, indices):
        return cls.data[cls.__from_indices(indices)]

    @classmethod
    def __get_prev(cls, indices):
        return cls.prev[cls.__from_indices(indices)]

    @classmethod
    def __set_lcs(cls, value, indices):
        cls.data[cls.__from_indices(indices)] = value

    @classmethod
    def __set_prev(cls, prev_indices, indices):
        cls.prev[cls.__from_indices(indices)] = cls.__from_indices(prev_indices)
