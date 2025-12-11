import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from nomad_actions.actions.simple_action.activities import greet
from nomad_actions.actions.simple_action.models import SimpleWorkflowInput
from nomad_actions.actions.simple_action.workflows import SimpleWorkflow


@pytest.mark.asyncio
async def test_simple_workflow():
    task_queue = 'test-simple-workflow'
    async with await WorkflowEnvironment.start_local() as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[SimpleWorkflow],
            activities=[greet],
        ):
            result = await env.client.execute_workflow(
                SimpleWorkflow.run,
                SimpleWorkflowInput(
                    upload_id='upload_id',
                    user_id='user_id',
                    name='World',
                ),
                id='test-workflow',
                task_queue=task_queue,
            )
            assert (
                result == 'hello World - created by user user_id for upload upload_id'
            )
