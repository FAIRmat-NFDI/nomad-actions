from pydantic import BaseModel, Field


class SearchWorkflowInput(BaseModel):
    """Input model for the simple workflow"""

    upload_id: str = Field(
        ...,
        description='Unique identifier for the upload associated with the workflow.',
    )
    user_id: str = Field(
        ..., description='Unique identifier for the user who initiated the workflow.'
    )

    query: dict = Field(..., description='Query for extracting entries.')
