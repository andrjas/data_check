import concurrent.futures
from typing import Callable, List, Any, Dict, Optional

from .checks.base_check import BaseCheck
from .result import DataCheckResult
from .output import DataCheckOutput


class DataCheckRunner:
    def __init__(self, workers: int, output: Optional[DataCheckOutput] = None) -> None:
        self.workers = workers
        if output is None:
            output = DataCheckOutput()
        self.output = output

    @property
    def executor(self):
        return concurrent.futures.ProcessPoolExecutor(max_workers=self.workers)

    def is_serial_run(self, run_list: List[Any]) -> bool:
        return self.workers == 1 or len(run_list) == 1

    def run_checks(
        self,
        run_method: Callable[[BaseCheck], DataCheckResult],
        all_checks: List[BaseCheck],
    ) -> List[DataCheckResult]:
        if self.is_serial_run(all_checks):
            return self.run_checks_serial(run_method, all_checks)
        else:
            return self.run_checks_parallel(run_method, all_checks)

    def run_checks_parallel(
        self,
        run_method: Callable[[BaseCheck], DataCheckResult],
        all_checks: List[BaseCheck],
    ) -> List[DataCheckResult]:
        """
        Runs all tests in parallel.
        Returns a list of the results
        """
        result_futures = [self.executor.submit(run_method, f) for f in all_checks]
        results: List[DataCheckResult] = []
        for future in concurrent.futures.as_completed(result_futures):
            dc_result = future.result()
            results.append(dc_result)
            self.output.print(dc_result)
        return results

    def run_checks_serial(
        self,
        run_method: Callable[[BaseCheck], DataCheckResult],
        all_checks: List[BaseCheck],
    ) -> List[DataCheckResult]:
        """
        Runs all tests in serial.
        Returns a list of the results
        """
        results: List[DataCheckResult] = []
        for f in all_checks:
            dc_result = run_method(f)
            results.append(dc_result)
            self.output.print(dc_result)
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
