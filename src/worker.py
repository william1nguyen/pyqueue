from task_queue import TaskQueue
from redis_client import RedisClient


class Worker:
    def __init__(self, redis: RedisClient, queue_name: str, handler):
        self.queue = TaskQueue(redis, queue_name)
        self.handler = handler

    def run(self):
        while True:
            job = self.queue.dequeue()
            if not job:
                continue
            try:
                self.handler(job.payload)
            except Exception as e:
                print("error: ", e)
