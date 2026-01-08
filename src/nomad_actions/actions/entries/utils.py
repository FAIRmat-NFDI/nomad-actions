import os
import pyarrow as pa
import pyarrow.parquet as pq
from nomad.utils import dict_to_dataframe


def write_parquet_file(path: str, data: list[dict]):
    """Writes a list of NOMAD entry dicts to a parquet file.

    Args:
        path (str): The path where the file will be saved.
        data (list[dict]): The list of NOMAD entry dicts to be written to the file.
        mode (str, optional): The write mode, either 'overwrite' or 'append'.
            Defaults to 'overwrite'.
    """
    if not path.endswith('parquet'):
        raise ValueError('Unsupported file type. Please use parquet.')

    df = dict_to_dataframe(data)

    table = pa.Table.from_pandas(df)
    with pq.ParquetWriter(
        path,
        table.schema,
        compression='snappy',  # snappy for faster write/read for individual files
        use_dictionary=True,
    ) as writer:
        writer.write_table(table)


def write_csv_file(path: str, data: list[dict]):
    """Writes a list of NOMAD entry dicts to a CSV file.

    Args:
        path (str): The path where the file will be saved.
        data (list[dict]): The list of NOMAD entry dicts to be written to the file.
        mode (str, optional): The write mode, either 'overwrite' or 'append'.
            Defaults to 'overwrite'.
    """
    if not path.endswith('csv'):
        raise ValueError('Unsupported file type. Please use CSV.')

    df = dict_to_dataframe(data)

    df.to_csv(path, index=False, mode='w', header=True)


def consolidate_files(input_file_paths: list[str], output_file_path: str):
    """Consolidates multiple Parquet or CSV files into a single file.

    Args:
        input_file_paths (list[str]): List of file paths to be consolidated.
        output_file_path (str): Path for the consolidated output file.
    """
    if output_file_path.endswith('parquet'):
        tables = []
        for file_path in input_file_paths:
            table = pq.read_table(file_path)
            tables.append(table)
        combined_table = pa.concat_tables(tables)
        pq.write_table(
            combined_table,
            output_file_path,
            compression='zstd',  # for better compression for consolidated file
            compression_level=3,
            use_dictionary=True,
        )
    elif output_file_path.endswith('csv'):
        import pandas as pd

        dataframes = []
        for file_path in input_file_paths:
            df = pd.read_csv(file_path)
            dataframes.append(df)
        combined_df = pd.concat(dataframes, ignore_index=True)
        combined_df.to_csv(output_file_path, index=False)
    else:
        raise ValueError('Unsupported file type. Please use parquet or CSV.')
