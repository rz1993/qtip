from collections import deque
from queue import PriorityQueue


class Storage(object):
    def __init__(self, name='cacheme', *args, **kwargs):
        self.name = name

    def enqueue(self, task_ser):
        """
        Add the task binary to the storage's task queue.
        """
        raise NotImplementedError

    def dequeue(self):
        """
        Remove the top task from the task queue.
        """
        raise NotImplementedError

    def unqueue(self, task_ser):
        """
        Remove task binary from the task queue.
        """
        raise NotImplementedError

    def queue_size(self):
        """
        Return the number of task binaries currently in the queue.
        """
        raise NotImplementedError

    def add_to_schedule(self, task_ser, ts):
        """
        Add the task binary to the storage schedule.
        """
        raise NotImplementedError

    def del_from_schedule(self, task_id, ts):
        """
        Delete a task binary from the schedule.
        """
        raise NotImplementedError

    def get_from_schedule(self, ts=None):
        """
        Get tasks in schedule before the inputted timestamp.
        """
        raise NotImplementedError

    def is_scheduled(self, task_id, ts):
        """
        Check if a task binary is scheduled for a specific timestamp.
        """
        raise NotImplementedError

    def read_from_schedule(self, limit=-1):
        """
        Read tasks from the schedule, up to an optional limit.
        """
        raise NotImplementedError

    def put_data(self, key, value):
        """
        Store a result for a specific task_id to be retrieved later.
        """
        raise NotImplementedError

    def get_data(self, key):
        """
        Get a value from the storage based on key without deleting it.
        """
        raise NotImplementedError

    def pop_data(self, key):
        """
        Get + delete.
        """
        raise NotImplementedError

    def flush_queue(self):
        """
        Remove all task binaries in the task queue.
        """
        raise NotImplementedError

    def flush_schedule(self):
        """
        Clear all tasks from the schedule.
        """
        raise NotImplementedError

    def flush_all(self):
        """
        Clear all tasks from the schedule.
        """
        raise NotImplementedError


class MemoryStorage(Storage):
    def __init__(self, name='cacheme', *args, **kwargs):
        self.name = name
        self.tasks = deque()
        self._schedule = PriorityQueue()
        self._deleted = {}
        self._data = {}

    def enqueue(self, task_ser):
        self.tasks.appendleft(task_ser)

    def dequeue(self):
        return self.tasks.pop()

    def unqueue(self, task_ser):
        try:
            self.tasks.remove(task_ser)
        except ValueError:
            return  # If the task doesn't exist in the queue

    def queue_size(self):
        return len(self.tasks)

    def add_to_schedule(self, task_ser, ts):
        self._schedule.put((ts, task_ser))

    def del_from_schedule(self, task_id, ts):
        self._deleted[task_ser] = ts

    def get_from_schedule(self, ts=None):
        """
        Since we can't randomly access Python's priority queue,
        we can instead artificially delete a task from the schedule
        by adding it to a hash lookup and ignoring it when we end up
        popping it from the priority queue.
        """
        task_ser = self._schedule.get()
        while task_ser in self._deleted:
            del self._deleted[task_ser]
            task_ser = self._schedule.get()
        return task_ser

    def is_scheduled(self, task_id, ts):
        pass

    def read_from_schedule(self, limit=-1):
        pass

    def put_data(self, key, value):
        if key not in self._data:
            self._data[key] = value

    def get_data(self, key):
        return self._data.get(key)

    def pop_data(self, key):
        if key in self._data:
            value = self._data.get(key)
            del self._data[key]
            return value
        return None

    def flush_queue(self):
        num_tasks = self.queue_size()
        self.tasks.clear()
        return num_tasks

    def flush_schedule(self):
        self._schedule = PriorityQueue()

    def flush_all(self):
        self.tasks.clear()
        self._schedule = PriorityQueue()
        self._data = {}
        self._revoked = {}
