from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pytest

from data_check.runner import DataCheckRunner, NoPoolExecutor


@pytest.mark.parametrize("workers", [0, 1, 2, 5, 10])
def test_executor_returns_dummy_executor_for_empty_task_list(workers: int):
    executor = DataCheckRunner(workers=workers).executor([])
    assert isinstance(executor, NoPoolExecutor)


@pytest.mark.parametrize("workers", [0, 1, 2, 5, 10])
def test_executor_returns_dummy_executor_for_task_list_with_one_task(workers: int):
    executor = DataCheckRunner(workers=workers).executor([1])
    assert isinstance(executor, NoPoolExecutor)


@pytest.mark.parametrize("task_size", [0, 1, 2, 5, 10])
def test_executor_returns_dummy_executor_if_workers_eq_1(task_size: int):
    executor = DataCheckRunner(workers=1).executor([1] * task_size)
    assert isinstance(executor, NoPoolExecutor)


@pytest.mark.parametrize(("workers", "task_size"), [(2, 2), (2, 10), (5, 2), (5, 10)])
def test_executor_returns_process_pool_executor_for_task_list_with_more_task(
    workers: int, task_size: int
):
    executor = DataCheckRunner(workers=workers, use_process=True).executor(
        [1] * task_size
    )
    assert isinstance(executor, ProcessPoolExecutor)


@pytest.mark.parametrize(("workers", "task_size"), [(2, 2), (2, 10), (5, 2), (5, 10)])
def test_executor_returns_thread_pool_executor_for_task_list_with_more_task(
    workers: int, task_size: int
):
    executor = DataCheckRunner(workers=workers).executor([1] * task_size)
    assert isinstance(executor, ThreadPoolExecutor)
