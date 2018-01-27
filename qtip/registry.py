import pickle


class TaskRegistry(object):
    """
    Simple in-memory registry for task classes. Allows task objects to be
    recreated from their deserialized parameters. Also serves as serialization
    and deserialization protocol for parameter data queried from queue storage.
    """
    def __init__(self):
        self._registry = {}
        self._periodic_tasks = []

    def get_class_key(self, task_class):
        return task_class.__name__

    def get_class_from_key(self, class_key):
        task_class = self._registry.get(class_key)

        if not task_class:
            raise Exception("{} not found in registry".format(class_key))

        return task_class

    def _register_task(self, task_class):
        class_key = self.get_class_key(task_class)

        if class_key not in self._registry:
            self._registry[class_key] = task_class

    def register_task(self, task_class):
        self._register_task(task_class)

    def register_periodic(self, task_class):
        # NOTE: To schedule same task class for different intervals has to be
        # done logically within the class' `ready` handler.
        if not hasattr('ready', task_class):
            raise Exception('Periodic tasks should have a `ready` handler to '
                            'verify timestamps. `{}` does not have a `ready`'
                            'handler.'.format(task_class.__name__))

        class_key = self.get_class_key(task_class)
        if class_key in self._registry and class_key in self._periodic_tasks:
            raise Exception('`{}` is already registered as a periodic'
                            'task.'.format(task_class.__name__))

        self._registry[class_key] = task_class
        self._periodic_tasks.append(task_class)

    def unregister_task(self, task_class):
        class_key = self.get_class_key(task_class)

        if class_key in self._registry:
            del self._registry[class_key]

    def periodic_tasks(self):
        return [self.get_class_from_key(k) for k in self._periodic_tasks]

    def get_ready_tasks(self, ts):
        return [task() for task in self.periodic_tasks() if task.ready(ts)]

    def serialize_task(self, task):
        """
        Serialize the task and all its runtime information.
        """
        class_key = self.get_class_key(type(task))

        return pickle.dumps((
            task.task_id,
            class_key,
            task.get_data(),
            task.retries,
            task.retry_delay,
            task.execute_time))

    def deserialize_task(self, str_data):
        """
        TODO: Add explicit payload validation, e.g. checking for tuple
        """
        payload = pickle.loads(str_data)

        if len(payload) == 6:
            task_id, class_key, data, retries, delay, exec_time = payload
        else:
            raise Exception("Invalid task. Queued tasks should be tuples of length 6.")

        task_class = self.get_class_from_key(class_key)

        return task_class(data=data,
            task_id=task_id,
            retries=retries,
            retry_delay=delay,
            execute_time=exec_time)
