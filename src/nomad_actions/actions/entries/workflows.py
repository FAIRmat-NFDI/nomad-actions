from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from nomad_actions.actions.entries.activities import search
    from nomad_actions.actions.entries.models import SearchWorkflowInput


@workflow.defn
class SearchWorkflow:
    @workflow.run
    async def run(self, data: SearchWorkflowInput) -> str:
        retry_policy = RetryPolicy(
            maximum_attempts=3,
        )
        result = await workflow.execute_activity(
            search,
            data,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=retry_policy,
        )
        return result
