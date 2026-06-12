from readers.bronze_reader import BronzeReader
from transformers.data_cleanser import DataCleanser
from transformers.data_type_converter import DataTypeConverter
from transformers.derived_fields import DerivedFields
from writers.silver_writer import SilverWriter
import argparse
import json
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def read_config(config_file):
    """Read and parse configuration file."""
    try:
        with open(config_file) as f:
            config = json.load(f)
        logger.info(f"Successfully loaded config from {config_file}")
        return config
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_file}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config file: {e}")
        raise
    except Exception as e:
        logger.error(f"Error reading config file: {e}")
        raise

def main():
    try:
        # Parse arguments
        parser = argparse.ArgumentParser(description="Silver layer data transformation pipeline")
        parser.add_argument('config_file', type=str, help='Path to JSON configuration file')
        args = parser.parse_args()
        
        # Read configuration
        logger.info("Starting silver layer transformation pipeline")
        config = read_config(args.config_file)
        
        # Extract config values
        source_table = config['source_table']
        target_table = config['target_table']
        primary_key = config.get('primary_key')
        required_columns = config.get('required_columns', [])
        drop_duplicates = config.get('drop_duplicates', False)
        remove_nulls = config.get('remove_nulls', False)
        capitalize_columns = config.get('capitalize_columns', [])
        convert_columns = config.get('convert_columns', {})
        derived_ts_columns = config.get('derived_ts_columns', [])
        drop_nulls_columns = config.get('drop_null_columns',[])
        
        logger.info(f"Processing {source_table} -> {target_table}")
        
        # Read source bronze table
        try:
            logger.info(f"Reading bronze table: {source_table}")
            df_bronze = BronzeReader.read_bronze(source_table)
            record_count = df_bronze.count()
            logger.info(f"Read {record_count} records from {source_table}")
        except Exception as e:
            logger.error(f"Failed to read bronze table {source_table}: {e}")
            raise
        
        # Data cleansing - remove nulls
        try:
            if remove_nulls:
                logger.info(f"Removing null values for colums: {drop_nulls_columns}")
                df_removed_nulls = DataCleanser.drop_nulls(df_bronze, drop_nulls_columns)
                records_after = df_removed_nulls.count()
                logger.info(f"Removed {record_count - records_after} records with nulls")
                record_count = records_after
            else:
                df_removed_nulls = df_bronze
                logger.info("Skipping null removal")
        except Exception as e:
            logger.error(f"Failed during null removal: {e}")
            raise
        
        # Remove duplicates
        try:
            if drop_duplicates:
                logger.info("Removing duplicate records")
                df_dropped_dups = DataCleanser.drop_duplicates(df_removed_nulls)
                records_after = df_dropped_dups.count()
                logger.info(f"Removed {record_count - records_after} duplicate records")
                record_count = records_after
            else:
                df_dropped_dups = df_removed_nulls
                logger.info("Skipping duplicate removal")
        except Exception as e:
            logger.error(f"Failed during duplicate removal: {e}")
            raise
        
        # Capitalize column values
        try:
            #logger.info("Capitalizing column values")
            logger.info(f"Capitalizing columns: {capitalize_columns}")
            if len(capitalize_columns) > 0:
                logger.info(f"Capitalizing columns: {capitalize_columns}")
                df_capitalized = DataCleanser.capitalized(df_dropped_dups, capitalize_columns)
                logger.info("Column capitalization completed")
            else:
                df_capitalized = df_dropped_dups
                logger.info("Skipping column capitalization")
        except Exception as e:
            logger.error(f"Failed during column capitalization: {e}")
            raise
        
        # Convert column data types
        try:
            if len(convert_columns) > 0:
                logger.info(f"Converting data types for columns: {list(convert_columns.keys())}")
                df_converted = DataTypeConverter.convert_columns(df_capitalized, convert_columns)
                logger.info("Data type conversion completed")
            else:
                df_converted = df_capitalized
                logger.info("Skipping data type conversion")
        except Exception as e:
            logger.error(f"Failed during data type conversion: {e}")
            raise
        
        # Add derived timestamp columns
        try:
            if len(derived_ts_columns) > 0:
                logger.info(f"Extracting date/time components from: {derived_ts_columns}")
                df_derived = DerivedFields.extract_dates_time(df_converted, derived_ts_columns)
                logger.info("Derived fields extraction completed")
            else:
                df_derived = df_converted
                logger.info("Skipping derived fields extraction")
        except Exception as e:
            logger.error(f"Failed during derived fields extraction: {e}")
            raise
        
        # Write to silver table
        try:
            logger.info(f"Writing {record_count} records to silver table: {target_table}")
            SilverWriter.write_to_table(df_derived, target_table)
            logger.info(f"Successfully completed transformation pipeline for {target_table}")
        except Exception as e:
            logger.error(f"Failed to write to silver table {target_table}: {e}")
            raise
            
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
