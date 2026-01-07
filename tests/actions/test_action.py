import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from nomad_actions.actions.entries.activities import search
from nomad_actions.actions.entries.models import SearchWorkflowInput
from nomad_actions.actions.entries.workflows import SearchWorkflow


@pytest.mark.asyncio
async def test_simple_workflow():
    task_queue = 'test-simple-workflow'
    async with await WorkflowEnvironment.start_local() as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[SearchWorkflow],
            activities=[search],
        ):
            result = await env.client.execute_workflow(
                SearchWorkflow.run,
                SearchWorkflowInput(
                    upload_id='upload_id',
                    user_id='user_id',
                    query={},
                ),
                id='test-workflow',
                task_queue=task_queue,
            )
            assert (
                result == 'hello World - created by user user_id for upload upload_id'
            )
