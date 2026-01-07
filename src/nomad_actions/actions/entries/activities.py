from temporalio import activity

from nomad_actions.actions.entries.models import SearchWorkflowInput


@activity.defn
async def search(data: SearchWorkflowInput) -> str:
    return f'hello {data.name} - created by user {data.user_id} for upload {data.upload_id}'
