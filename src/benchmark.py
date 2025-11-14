import time
import threading
from .connection import RedisConnection
from .worker import Worker


class QuickBenchmark:
    def __init__(self):
        self.connection = RedisConnection(
            host="localhost", port=6379, password="redisadmin"
        )
        self.completed = 0
        self.lock = threading.Lock()

    def find_max_throughput(self):
        configurations = [
            {"workers": 4, "jobs": 2000, "work_ms": 10},
            {"workers": 8, "jobs": 5000, "work_ms": 10},
            {"workers": 16, "jobs": 10000, "work_ms": 10},
            {"workers": 32, "jobs": 20000, "work_ms": 10},
            {"workers": 16, "jobs": 10000, "work_ms": 5},
            {"workers": 32, "jobs": 20000, "work_ms": 5},
        ]

        results = []

        for config in configurations:
            result = self._run_test(**config)
            results.append({**config, **result})
            self.connection.client.flushdb()
            time.sleep(1)

        print("Workers\tJobs\tWork(ms)\tJobs/min\tSuccess%")
        for r in results:
            print(
                f"{r['workers']}\t{r['jobs']}\t{r['work_ms']}\t"
                f"{r['jobs_per_minute']:.0f}\t{r['success_rate']:.1f}"
            )

        best = max(results, key=lambda x: x["jobs_per_minute"])
        print(
            f"\nBest: {best['jobs_per_minute']:.0f} jobs/min "
            f"({best['workers']} workers, {best['work_ms']}ms)"
        )

        return results

    def _run_test(self, workers: int, jobs: int, work_ms: int):
        self.completed = 0

        worker_list = []
        for _ in range(workers):
            w = Worker(self.connection, queue_name="benchmark", concurrency=1)

            @w.process("bench_job")
            def process_bench(payload):
                time.sleep(work_ms / 1000)
                with self.lock:
                    self.completed += 1
                return True

            worker_list.append(w)

        for w in worker_list:
            t = threading.Thread(target=w.start)
            t.daemon = True
            t.start()

        start = time.time()
        for i in range(jobs):
            worker_list[0].queue.add("bench_job", {"id": i})

        while self.completed < jobs:
            time.sleep(0.2)

        end = time.time()

        for w in worker_list:
            w.stop()

        duration = end - start
        jobs_per_minute = (jobs / duration) * 60
        success_rate = (self.completed / jobs) * 100

        return {
            "duration": duration,
            "jobs_per_second": jobs / duration,
            "jobs_per_minute": jobs_per_minute,
            "success_rate": success_rate,
        }

    def cleanup(self):
        self.connection.client.flushdb()
        self.connection.close()


def main():
    benchmark = QuickBenchmark()
    try:
        benchmark.find_max_throughput()
    finally:
        benchmark.cleanup()


if __name__ == "__main__":
    main()
