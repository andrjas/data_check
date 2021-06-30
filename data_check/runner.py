import concurrent.futures
from typing import Callable, List, Any, Dict
from pathlib import Path


from .result import DataCheckResult


class DataCheckRunner:
    def __init__(self, workers: int) -> None:
        self.workers = workers

    @property
    def executor(self):
        return concurrent.futures.ProcessPoolExecutor(max_workers=self.workers)

    def is_serial_run(self, run_list: List[Any]) -> bool:
        return self.workers == 1 or len(run_list) == 1

    def run(
        self, run_method: Callable[[Path], DataCheckResult], all_files: List[Path]
    ) -> List[DataCheckResult]:
        if self.is_serial_run(all_files):
            return self.run_serial(run_method, all_files)
        else:
            return self.run_parallel(run_method, all_files)

    def run_parallel(
        self, run_method: Callable[[Path], DataCheckResult], all_files: List[Path]
    ) -> List[DataCheckResult]:
        """
        Runs all tests in parallel.
        Returns a list of the results
        """
        result_futures = [self.executor.submit(run_method, f) for f in all_files]
        results: List[DataCheckResult] = []
        for future in concurrent.futures.as_completed(result_futures):
            dc_result = future.result()
            results.append(dc_result)
            print(dc_result.message)
        return results

    def run_serial(
        self, run_method: Callable[[Path], DataCheckResult], all_files: List[Path]
    ) -> List[DataCheckResult]:
        """
        Runs all tests in serial.
        Returns a list of the results
        """
        results: List[DataCheckResult] = []
        for f in all_files:
            dc_result = run_method(f)
            results.append(dc_result)
            print(dc_result.message)
        return results

    def run_any(
        self, run_method: Callable[..., Any], parameters: List[Dict[str, Any]]
    ) -> List[Any]:
        if self.is_serial_run(parameters):
            return self.run_any_serial(run_method, parameters)
        else:
            return self.run_any_parallel(run_method, parameters)

    def run_any_serial(
        self, run_method: Callable[..., Any], parameters: List[Dict[str, Any]]
    ) -> List[Any]:
        return [run_method(**p) for p in parameters]

    def run_any_parallel(
        self, run_method: Callable[..., Any], parameters: List[Dict[str, Any]]
    ) -> List[Any]:
        result_futures = [self.executor.submit(run_method, **p) for p in parameters]
        results: List[Any] = []
        for future in concurrent.futures.as_completed(result_futures):
            dc_result = future.result()
            results.append(dc_result)
        return results
