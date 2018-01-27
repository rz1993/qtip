import uuid

from datetime import datetime, timedelta

from consumer import Consumer
from registry import TaskRegistry


class Producer(object):
    def __init__(self, task_registry=None, store_results=True, **storage_kwargs):
        self.storage = self.get_storage(**storage_kwargs)
        self.store_results = store_results

        self.task_registry = TaskRegistry() if task_registry is None else task_registry

    def get_storage(self, **kwargs):
        raise NotImplementedError('Backend for base `Producer` class not specified. '
                                  'Use a subclass instead.')

    def get_consumer(self, **config):
        return Consumer(self, **config)
        
    """ Task Decorators """

    def task(self, blocking=False, retries=0, retry_delay=0, name=None,
             **task_config):
        """
        Return a decorator that can be called to enqueue function execution
        """
        def decorator(f):
            return TaskGenerator(self, f,
                retries=retries,
                retry_delay=retry_delay,
                lock=lock,
                name=name,
                **task_config)
        return decorator

    def periodic_task(self, blocking=False, retries=0, retry_delay=0, name=None,
                      ready_handler=None, **task_config):
        """
        Return a decorator that can be called to add a reoccuring, periodic task.
        """
        def decorator(f):
            return TaskGenerator(self, f,
                retries=retries,
                retry_delay=retry_delay,
                lock=lock,
                name=name,
                ready=ready_handler,
                periodic=True)
        return decorator

    """ Enqueueing, scheduling and backend data handling. """

    def _enqueue(self, task_ser):
        self.storage.enqueue(task_ser)

    def enqueue(self, task):
        if not isinstance(task, QueuedTask):
            raise Exception('`{}` is an invalid task type. `task` must be an '
                            'instance of a subclass of `QueuedTask`.')
        self.register_task(type(task))

        task_ser = self.task_registry.serialize_task(task)
        self._enqueue(task_ser)

        return TaskResult(self, task)

    def dequeue(self):
        task_ser = self.storage.dequeue()
        task = self.task_registry.deserialize_task(task_ser)

        return task

    def requeue(self, task):
        self._enqueue(task)

    def add_to_schedule(self, task, ts):
        task_ser = self.task_registry.serialize_task(task)
        self.storage.add_to_schedule(task_ser, ts)

    def get_from_schedule(self, ts):
        task_list = self.storage.get_from_schedule(ts)
        return [self.task_registry.deserialize_task(t) for t in task_list]

    def restart(self, task):
        pass

    def revoke(self, task):
        task_ser = self.task_registry.serialize_task(task)
        self.storage.unqueue(task_ser)

    def get_data(self, task, preserve=False):
        if preserve:
            return self.storage.get_data(task.task_id)
        return self.storage.pop_data(task.task_id)

    def put_data(self, task, data):
        self.storage.put_data(task.task_id, data)

    """ Task Registry handling. """

    def register_task(self, task_class):
        if task_class.periodic:
            self.task_registry.register_periodic(task_class)
        else:
            self.task_registry.register_task(task_class)

    def unregister_task(self, task_class):
        self.task_registry.unregister_task(task_class)

    def read_periodic_tasks(self):
        return self.task_registry.periodic_tasks()

    def get_ready_tasks(self, ts=None):
        if ts is None:
            ts = datetime.utcnow()
        return self.task_registry.get_ready_tasks(ts)

    """ Task execution handling. """

    def execute(self, task):
        # TODO: Add exception handling
        res = task.execute()
        if not self.store_results:
            return res

        self.put_data(task, res)

    def check_ready(self, task, ts):
        return True


class TaskGenerator(object):
    """
    Factory pattern for enqueueing `QueuedTask` instances everytime
    a factory instance is called. Retries and other task settings are
    separated from task functionality and stored at the factory level.
    Every `TaskGenerator` instance has an associated `QueuedTask` subclass
    that encapsulates the task's functionality. This is stored in a global
    registry so that it can be retrieved at execution time.
    """

    def __init__(self, producer, func, retries=0, retry_delay=0,
                 name=None, task_class=None, **config):
        self.producer = producer
        self.retries = retries
        self.retry_delay = retry_delay
        self.task_class = create_task_class(
            QueuedTask if task_class is None else task_class,
            func,
            task_name=name,
            **config)
        self.producer.register_task(self.task_class)

    def schedule(self, exec_time=None, delay=0, args=[], kwargs={}):
        """
        Dynamically schedule or delay a task with arguments.
        TODO: Write utility functions for dealing with date and time
        TODO: Dealing with empty cases for args and kwargs
        """
        task = self.task_class((args, kwargs), retries=self.retries,
            retry_delay=self.retry_delay)
        if exec_time is None or not isinstance(exec_time, datetime):
            exec_time = datetime.utcnow() + timedelta(seconds=delay)

        self.producer.add_to_schedule(task, exec_time)

    def __call__(self, *args, **kwargs):
        task_instance = self.task_class((args, kwargs),
            retries=self.retries, retry_delay=self.retry_delay)
        return self.producer.enqueue(task_instance)


class QueuedTask(object):
    """
    Object class encapsulating task functionality, settings and arguments
    defined at task calltime.
    """
    def __init__(self, data=None, task_id=None, retries=0, retry_delay=0,
                execute_time=None, tries=0, **kwargs):
        self.task_id = task_id or self.gen_id()
        self.data = None
        self.set_data(data)
        self.execute_time = execute_time
        self.retries = retries
        self.tries = tries
        self.retry_delay = retry_delay

    def __eq__(self, task):
        return (self.task_id == task.task_id and
                self.execute_time == task.execute_time and
                type(self) == type(task))

    def gen_id(self):
        return str(uuid.uuid4())

    def get_data(self):
        return self.data

    def extend_data(self, args, kwargs):
        old_args, old_kwargs = self.get_data() or ((), {})
        if isinstance(args, tuple):
            old_args.extend(args)
        else:
            raise TypeError('New arguments must be a tuple.')

        if isinstance(kwargs, dict):
            old_kwargs.update(kwargs)
        else:
            raise TypeError('New keyword arguments must be a dictionary.')

        data = (old_args, old_kwargs)
        self.set_data(data)

    def ready(self, ts):
        return self.execute_time is None or ts > self.execute_time

    def set_data(self, data):
        self.data = data

    def set_execute_time(self, ts):
        self.execute_time = ts

    def execute(self):
        raise NotImplementedError('The `execute` method must be implemented. '
                                  'Please use a subclass of `QueuedTask`.')


def create_task_class(base_class, func, task_name=None, **config):
    """
    Dynamically subclass a `task_class` by tying its execution with `func`.
    """
    def execute(self):
        args, kwargs = self.get_data() or ((), {})
        return func(*args, **kwargs)

    attrs = {
        'execute': execute,
        '__module__': func.__module__,
        '__doc__': func.__doc__
    }
    attrs.update(config)

    if not task_name:
        task_name = 'queued_task_{}'.format(func.__name__)

    return type(task_name, (base_class,), attrs)


class TaskResult(object):
    """
    Result object returned from task execution if execution is
    non-blocking. Behaves similar to a promise and allows for the
    result of the task to be retrieved programmatically.
    """
    def __init__(self, producer, task):
        self.producer = producer
        self.task = task

        self._result = None

    def _get_raw_result(self, blocking=False, timeout=5):
        if blocking:
            start = datetime.time()
            result = None
            while result is None:
                result = self.producer.get_data(self.task)
                if datetime.time() - start > timeout:
                    break
            return result
        else:
            return self.producer.get_data(self.task)

    def get_result(self, blocking=False):
        """
        Explicit is better than implicit. We check the storage if
        no result has been retrieved and update the result if it is
        in the storage.
        """
        # TODO: Change this to an explicit data type rather than `None`.
        if self._result is None:
            result = self._get_raw_result(blocking)
            if result is not None:
                self._result = self.load_data(result)
                return self._result
            else:
                return result
        else:
            return self._result

    def revoke(self):
        self.producer.revoke(self.task)

    def restart(self):
        return self.producer.restart(self.task)

    def load_data(self, data):
        pass
