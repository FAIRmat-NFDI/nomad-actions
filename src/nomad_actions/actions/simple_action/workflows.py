from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from nomad_actions.actions.simple_action.activities import greet
    from nomad_actions.actions.simple_action.models import SimpleWorkflowInput


@workflow.defn
class SimpleWorkflow:
    @workflow.run
    async def run(self, data: SimpleWorkflowInput) -> str:
        retry_policy = RetryPolicy(
            maximum_attempts=3,
        )
        result = await workflow.execute_activity(
            greet,
            data,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=retry_policy,
        )
        return result
