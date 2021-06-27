import concurrent.futures


class DataCheckRunner:
    def __init__(self, workers: int) -> None:
        self.workers = workers

    @property
    def executor(self):
        return concurrent.futures.ProcessPoolExecutor(max_workers=self.workers)

    def is_serial_run(self, all_files) -> bool:
        return self.workers == 1 or len(all_files) == 1

    def run(self, run_method, all_files):
        if self.is_serial_run(all_files):
            return self.run_serial(run_method, all_files)
        else:
            return self.run_parallel(run_method, all_files)

    def run_parallel(self, run_method, all_files):
        """
        Runs all tests in parallel.
        Returns a list of the results
        """
        result_futures = [self.executor.submit(run_method, f) for f in all_files]
        results = []
        for future in concurrent.futures.as_completed(result_futures):
            dc_result = future.result()
            results.append(dc_result)
            print(dc_result.message)
        return results

    def run_serial(self, run_method, all_files):
        """
        Runs all tests in serial.
        Returns a list of the results
        """
        results = []
        for f in all_files:
            dc_result = run_method(f)
            results.append(dc_result)
            print(dc_result.message)
        return results
