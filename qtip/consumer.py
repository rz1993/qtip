import threading
import time

from datetime import datetime, timedelta

# TODO: Add handling for revoked functions


class BaseProcess:
    def __init__(self, name=None, producer=None):
        self.name = name
        self.producer = producer

    def get_time(self):
        return datetime.utcnow()

    def loop(self):
        """
        Functionality for one loop of the process. By encapsulating only
        the loop logic, we can programmatically define our workers/schedulers
        as threads, processes or any other concurrent environment form.
        """
        raise NotImplementedError

    def requeue_task(self, task):
        self.producer.requeue(task)


class Worker(BaseProcess):
    def __init__(self, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)
        self._logger = None

    def loop(self):
        task = self.producer.dequeue()

        try:
            self.process_task(task)

    def process_task(self, task):
        # Check producer level ready conditions
        if not self.producer.check_ready(task, ts):
            self.requeue_task(task)

        # Check task level ready conditions
        ts = self.get_time()
        if not task.ready(ts):
            self.requeue_task(task)

        try:
            self.producer.execute(task)
        except Exception as FailedExec:
            # TODO: Add logging for failed execution.
            self.handle_retry(task, ts=self.get_time())
            raise FailedExec

    def handle_retry(self, task, ts):
        # TODO: Add some kind of logging for exceeding max retries
        if task.tries >= task.retries:
            return

        task.tries += 1
        if task.retry_delay:
            time_to_retry = task.retry_delay - (self.get_time() - ts)
            if time_to_retry > 0:
                time.sleep(time_to_retry)

        self.requeue_task(task)


class Scheduler(BaseProcess):
    def __init__(self, *args, periodic=True, **kwargs):
        self.periodic = periodic
        super(Scheduler, self).__init__(*args, **kwargs)

    def loop(self):
        now = self.get_time()
        scheduled_tasks = self.producer.get_from_schedule(now)

        for task in scheduled_tasks:
            if task.ready(now):
                self.enqueue(task)

        if self.periodic:
            self.enqueue_periodic_tasks()

    def enqueue(self, task):
        self.producer.enqueue(task)

    def enqueue_periodic_tasks(self):
        """
        Enqueue any periodic tasks that are ready.
        """
        now = self.get_time()

        ready_periodic_tasks = self.producer.get_ready_tasks(now)
        for task in ready_periodic_tasks:
            self.enqueue(task)


class ThreadFactory:
    def create_stop_flag(self):
        return threading.Event()

    def create_process(self, runnable, **kwargs):
        t = threading.Thread(target=runnable)
        t.daemon = True

        return t

class Consumer:
    def __init__(self, producer, name=None, workers=1, worker_factory=None,
                 **config):
        self.producer = producer
        self.name = name or 'cacheme.Consumer'
        self.worker_factory = worker_factory or self._create_factory()

        self.scheduler = self._create_scheduler()
        self.workers = [self._create_worker(i) for i in range(workers)]

        self.stop_flag = self.worker_factory.create_stop_flag()

        self.scheduler_proc = self.create_process(scheduler)
        self.worker_procs = [self.create_process(w) for w in self.workers]

    def _create_factory(self):
        return ThreadFactory()

    def _create_scheduler(self):
        return Scheduler(name="{}.Scheduler".format(self.name),
            periodic=True,
            producer=self.producer)

    def _create_worker(self, index):
        return Worker(name="{}.Worker_{}".format(self.name, index),
            producer=self.producer)

    def create_process(self, loopable):
        """
        Define a runnable that is dependent on the stop flag and pass
        it as the target for new process.
        """
        if not hasattr(loopable, 'loop'):
            raise Exception('`loopable` must implement a `loop` method.')

        def run():
            while not self.stop_flag.is_set():
                loopable.loop()
            return

        return self.worker_factory.create_process(run)

    def start(self):
        self.scheduler_proc.start()
        for worker in self.worker_procs:
            worker.start()

    def stop(self, graceful=True):
        self.stop_flag.set()

        if graceful:
            print("Shutting down gracefully...")
            for proc in self.worker_procs:
                proc.join()
            print("All threads shutdown.")

    def run(self):
        self.start()

        try:
            while True:
                print("[{}] Consumer running...".format(datetime.utcnow()))
                time.sleep(60)
        except KeyboardInterrupt:
            print("Shutting down gracefully...")
            self.stop(graceful=True)
