from data_check.config import DataCheckConfig

from .step import Step


class PingStep(Step):
    timeout: int = DataCheckConfig.wait_timeout
    retry: int = DataCheckConfig.wait_retry

    def run(self):
        return self.data_check.sql.test_connection(
            wait=True, timeout=self.timeout, retry=self.retry
        )
