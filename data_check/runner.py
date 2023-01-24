import multiprocessing as mp
import sys
from concurrent.futures import Executor, Future, ProcessPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Tuple

from .checks.base_check import BaseCheck
from .output import DataCheckOutput
from .result import DataCheckResult

if sys.version_info >= (3, 9):
    SubmitResult = Future[Any]
else:
    SubmitResult = Future


class NoPoolExecutor(Executor):
    def submit(  # type: ignore
        self, fn: Callable[..., Any], *args: Tuple[Any], **kwargs: Dict[str, Any]
    ) -> SubmitResult:
        f: SubmitResult = Future()
        try:
            result = fn(*args, **kwargs)
        except BaseException as e:
            f.set_exception(e)
        else:
            f.set_result(result)
        return f


class DataCheckRunner:
    def __init__(self, workers: int, output: Optional[DataCheckOutput] = None) -> None:
        self.workers = workers
        if output is None:
            output = DataCheckOutput()
        self.output = output

    def executor(self, task_list: List[Any]) -> Executor:
        max_new_workers = self.workers - len(mp.get_context().active_children())
        max_new_workers = min(max_new_workers, len(task_list))

        # Makes no sense to create a single worker
        # if we can process the work in this process:
        if max_new_workers > 1:
            return ProcessPoolExecutor(max_workers=max_new_workers)
        return NoPoolExecutor()

    def run_checks(
        self,
        run_method: Callable[[BaseCheck], DataCheckResult],
        all_checks: List[BaseCheck],
    ) -> List[DataCheckResult]:
        """
        Runs all tests.
        Returns a list of the results
        """
        executor = self.executor(all_checks)
        results: List[DataCheckResult] = []
        try:
            result_futures = [executor.submit(run_method, f) for f in all_checks]
            for future in as_completed(result_futures):
                dc_result = future.result()
                results.append(dc_result)
                self.output.print(dc_result)
        except KeyboardInterrupt:
            executor.shutdown(wait=False)
            for f in result_futures:
                f.cancel()
        return results

    def run_any(
        self, run_method: Callable[..., Any], parameters: List[Dict[str, Any]]
    ) -> List[Any]:
        executor = self.executor(parameters)
        results: List[Any] = []
        try:
            result_futures = [executor.submit(run_method, **p) for p in parameters]
            for future in as_completed(result_futures):
                dc_result = future.result()
                results.append(dc_result)
        except KeyboardInterrupt:
            executor.shutdown(wait=False)
            for f in result_futures:
                f.cancel()
        return results
