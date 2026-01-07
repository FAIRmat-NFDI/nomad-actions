from nomad.actions import TaskQueue
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from nomad.config.models.plugins import ActionEntryPoint


class SearchActionEntryPoint(ActionEntryPoint):
    def load(self):
        from nomad.actions import Action

        from nomad_actions.actions.entries.activities import search
        from nomad_actions.actions.entries.workflows import SearchWorkflow

        return Action(
            task_queue=self.task_queue,
            workflow=SearchWorkflow,
            activities=[search],
        )


search_action_entry_point = SearchActionEntryPoint(
    name='SearchAction',
    description='An action to search for entries based on a given query.',
    task_queue=TaskQueue.CPU,
)
