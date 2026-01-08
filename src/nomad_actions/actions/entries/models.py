import ast
from typing import Literal

from nomad.app.v1.models.models import MetadataPagination, MetadataRequired, Query
from pydantic import BaseModel, Field

OwnerLiteral = Literal['public', 'visible', 'shared', 'user', 'staging']
OutputFileTypeLiteral = Literal['parquet', 'csv']
IndexLiteral = Literal['entries', 'datasets', 'models', 'spaces']


class SearchWorkflowUserInput(BaseModel):
    """User Input model for search workflow"""

    upload_id: str = Field(
        ...,
        description='Unique identifier for the upload associated with the workflow.',
    )
    user_id: str = Field(
        ..., description='Unique identifier for the user who initiated the workflow.'
    )

    owner: OwnerLiteral | None = Field(
        'visible', description='Owner of the entries to be searched.'
    )
    query: str = Field(
        ...,
        description='Query for extracting entries.',
        json_schema_extra={
            'ui:widget': 'textarea',  # Explicitly request textarea widget
            'ui:options': {
                'rows': 5  # Optional: control height
            },
        },
    )
    required: str | None = Field(
        None,
        description='Required fields for filtering the search results.',
        json_schema_extra={
            'ui:widget': 'textarea',  # Explicitly request textarea widget
            'ui:options': {
                'rows': 5  # Optional: control height
            },
        },
    )
    output_file_type: OutputFileTypeLiteral | None = Field(
        'parquet',
        description='Type of the output file to be generated.',
    )


class CreateArtifactSubdirectoryInput(BaseModel):
    subdir_name: str = Field(..., description='Name of the subdirectory to be created.')


class SearchInput(BaseModel):
    user_id: str = Field(..., description='User ID performing the search.')
    owner: OwnerLiteral = Field(..., description='Owner of the entries to be searched.')
    query: Query = Field(..., description='Search query parameters.')
    required: MetadataRequired | None = Field(
        None, description='Required fields for filtering the search results.'
    )
    output_file_type: OutputFileTypeLiteral = Field(
        ..., description='Type of the output file to be generated.'
    )
    pagination: MetadataPagination = Field(
        ..., description='Pagination settings for the search results.'
    )
    output_dir: str = Field(..., description='Name of the output directory.')

    @classmethod
    def from_user_input(
        cls, user_input: SearchWorkflowUserInput, /, output_dir
    ) -> 'SearchInput':
        """Convert from SearchWorkflowUserInput to SearchInput"""
        print(user_input.query)
        query = ast.literal_eval(user_input.query)
        # required = (
        #     MetadataRequired.model_validate_json(user_input.required)
        #     if user_input.required
        #     else None
        # )
        pagination = MetadataPagination()  # Default pagination settings

        return cls(
            user_id=user_input.user_id,
            owner=user_input.owner,
            query=query,
            output_file_type=user_input.output_file_type,
            required=None,
            pagination=pagination,
            output_dir=output_dir,
        )


class ConsolidateOutputFilesInput(BaseModel):
    generated_file_paths: list[str] = Field(
        ..., description='List of the generated file paths to be consolidated.'
    )


class SearchWorkflowOutput(BaseModel):
    data: dict
