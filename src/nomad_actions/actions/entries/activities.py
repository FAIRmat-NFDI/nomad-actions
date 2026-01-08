import os

from temporalio import activity

from nomad_actions.actions.entries.models import (
    ConsolidateOutputFilesInput,
    CreateArtifactSubdirectoryInput,
    SearchInput,
)


@activity.defn
async def create_artifact_subdirectory(data: CreateArtifactSubdirectoryInput) -> str:
    """
    Creates a subdirectory within the action artifacts directory.

    Args:
        data (CreateArtifactSubdirectoryInput): Input data for creating subdirectory.

    Returns:
        str: Path to the created subdirectory.
    """
    from nomad.actions.manager import action_artifacts_dir

    subdir_path = os.path.join(action_artifacts_dir(), data.subdir_name)

    assert not os.path.exists(subdir_path)
    os.makedirs(subdir_path)

    return subdir_path


@activity.defn
async def search(data: SearchInput) -> list[str]:
    """
    Activity to perform NOMAD search based on the provided input data. The search
    results are written to a file in the specified format (Parquet or CSV) in the
    artifacts directory.

    Args:
        data (SearchInput): Input data for the search activity.

    Returns:
        list[str]: List of the generated output file paths.
    """
    from nomad.search import search

    from nomad_actions.actions.entries.utils import write_csv_file, write_parquet_file

    logger = activity.logger

    if data.output_file_type == 'parquet':
        write_dataset_file = write_parquet_file
    elif data.output_file_type == 'csv':
        write_dataset_file = write_csv_file
    else:
        raise ValueError('Unsupported file format. Please use parquet or csv.')

    generated_file_paths = []

    # first query
    search_counter = 1
    response = search(
        user_id=data.user_id,
        owner=data.owner,
        query=data.query,
        required=data.required,
        pagination=data.pagination,  # full pagination support can be added later
        aggregations={},  # aggregations support can be added later
    )
    output_filepath = os.path.join(
        data.output_dir, str(search_counter) + '.' + data.output_file_type
    )
    write_dataset_file(path=output_filepath, data=response.data)
    generated_file_paths.append(output_filepath)
    logger.info(
        f'Page {response.pagination.page} containing {len(response.data)} results '
        f'written to output file {output_filepath}.'
    )
    # subsequent queries if pagination is present
    while response.pagination and response.pagination.next_page_after_value:
        search_counter += 1
        response.pagination.page_after_value = response.pagination.next_page_after_value
        response = search(
            user_id=data.user_id,
            owner=data.owner,
            query=data.query,
            required=data.required,
            pagination=response.pagination,
            aggregations={},
        )
        output_filepath = os.path.join(
            data.output_dir, str(search_counter) + '.' + data.output_file_type
        )
        write_dataset_file(path=output_filepath, data=response.data)
        generated_file_paths.append(output_filepath)
        logger.info(
            f'Page {response.pagination.page} containing {len(response.data)} results '
            f'written to output file {output_filepath}.'
        )
    logger.info(f'Search completed. Output files available at: {data.output_dir}')

    return generated_file_paths


@activity.defn
async def consolidate_output_files(data: ConsolidateOutputFilesInput) -> str:
    """
    Activity to consolidate multiple Parquet or CSV files into a single file.

    Args:
        data (ConsolidateOutputFilesInput): Input data for consolidating files.

    Returns:
        str: Path to the consolidated output file.
    """
    from nomad_actions.actions.entries.utils import consolidate_files

    consolidated_file_path = os.path.join(
        os.path.dirname(data.generated_file_paths[0]),
        'consolidated_output.' + data.generated_file_paths[0].split('.')[-1],
    )

    consolidate_files(data.generated_file_paths, consolidated_file_path)

    return consolidated_file_path
