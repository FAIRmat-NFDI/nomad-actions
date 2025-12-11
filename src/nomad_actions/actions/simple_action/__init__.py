from nomad.actions import TaskQueue
from pydantic import Field
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from nomad.config.models.plugins import ActionEntryPoint


class SimpleActionEntryPoint(ActionEntryPoint):
    task_queue: str = Field(
        default=TaskQueue.CPU, description='Determines the task queue for this action'
    )

    def load(self):
        from nomad.actions import Action

        from nomad_actions.actions.simple_action.activities import greet
        from nomad_actions.actions.simple_action.workflows import SimpleWorkflow

        return Action(
            task_queue=self.task_queue,
            workflow=SimpleWorkflow,
            activities=[greet],
        )


simple_action_entry_point = SimpleActionEntryPoint(
    name='SimpleAction',
    description='A simple action that returns a greeting.',
)
