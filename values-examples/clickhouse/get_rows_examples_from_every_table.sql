from scripts.clickhouse_utils import ClickHouseClient
from clickhouse_connect.driver.exceptions import ClickHouseError
import logging
import sys
import time

# Configuration settings for the script
CONFIG = {
    "inserting_custom_datasets": True,  # Change to True for custom datasets, False for default datasets
    "table_to_insert_into": 'your_table_name_here',
    "query_list_of_datasets_to_insert": """
        SELECT
            CONCAT(schema_name, '.', report_dataset_name) AS relation_name,
            schema_name,
            report_dataset_name
        FROM system.tables
    """,
    "initial_order_by_and_limit": 'ORDER BY rand() LIMIT 4000',
    "maximum_columns": 900,
    "minimum_rows_to_insert": 100,
    "logfile_name": 'logfile.txt'
}

# Initialize logging
def setup_logging():
    logging.basicConfig(
        filename=CONFIG["logfile_name"],
        filemode='a',
        format='[%(levelname)s] %(asctime)s %(message)s',
        level=logging.INFO
    )
    print(f'\nLogs are being written to {CONFIG["logfile_name"]}\n')

# Create ClickHouse client
def get_clickhouse_client():
    client_type = 'CustomDB' if CONFIG["inserting_custom_datasets"] else 'DefaultDB'
    return ClickHouseClient(client_type)

# Main function to execute the script
def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    ch_client = get_clickhouse_client()
    start_time = time.time()

    datasets = retrieve_list_of_datasets_to_insert(ch_client, logger)
    process_datasets(datasets, ch_client, logger)

    log_script_completion(start_time, logger)

# Retrieve the list of datasets to process
def retrieve_list_of_datasets_to_insert(ch_client, logger):
    logger.info('Searching for datasets to insert...\n')
    try:
        datasets = ch_client.execute_query(CONFIG["query_list_of_datasets_to_insert"]).result_rows
        if not datasets:
            logger.warning('No datasets found! Exiting...\n')
            sys.exit(0)
        logger.info(f'Number of datasets found: {len(datasets)}\n')
        return datasets
    except ClickHouseError as e:
        logger.error(f'Failed to retrieve datasets: {e}')
        sys.exit(1)

# Process each dataset and attempt to insert it
def process_datasets(datasets, ch_client, logger):
    for dataset in datasets:
        relation_name, schema_name, table_name = dataset
        process_single_dataset(relation_name, schema_name, table_name, ch_client, logger)

# Process a single dataset
def process_single_dataset(relation_name, schema_name, table_name, ch_client, logger):
    try:
        column_names = query_dataset_columns(relation_name, ch_client, logger)
        if len(column_names) > CONFIG["maximum_columns"]:
            logger.info(f'Too many columns in {relation_name}, truncating to first {CONFIG["maximum_columns"]}')
            column_names = column_names[:CONFIG["maximum_columns"]]
        insert_dataset(relation_name, schema_name, table_name, column_names, ch_client, logger)
    except ClickHouseError as e:
        logger.error(f'Error processing dataset {relation_name}: {e}')

# Query dataset for column names
def query_dataset_columns(relation_name, ch_client, logger):
    query = f'SELECT * FROM {relation_name} LIMIT 0'
    logger.info(f'Querying {relation_name} for column names.')
    return ch_client.execute_query(query).column_names

# Insert data from dataset into the target table
def insert_dataset(relation_name, schema_name, table_name, column_names, ch_client, logger):
    content_query = build_content_query(relation_name, schema_name, table_name, column_names)
    try:
        logger.info(f'Inserting content from {relation_name}.')
        content = ch_client.execute_query(content_query).result_rows
        if content:
            ch_client.insert_data(CONFIG["table_to_insert_into"], data=content, column_names=column_names)
            logger.info(f'Successfully inserted content from {relation_name}.')
        else:
            logger.info(f'No content to insert from {relation_name}.')
    except Exception as e:
        logger.error(f'Failed to insert content from {relation_name}: {e}')

# Build the query to retrieve content for insertion
def build_content_query(relation_name, schema_name, table_name, column_names):
    column_selection = ' , '.join([f"'{column}', assumeNotNull(toString({column}))" for col in column_names])
    return f"""
        SELECT
            '{schema_name}' AS schema_name,
            '{table_name}' AS table_name,
            map({column_selection}) AS report_content_map
        FROM {relation_name}
        {CONFIG["initial_order_by_and_limit"]}
    """

def log_script_completion(start_time, logger):
    elapsed_time = time.time() - start_time
    logger.info(f'The script took {elapsed_time:.0f} seconds to complete.')

if __name__ == '__main__':
    main()
