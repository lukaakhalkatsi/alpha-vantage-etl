from etl.extract import extract
from etl.transform import StockDataTransformer
from etl.load import StockDataLoader
from utils.logger import logger
from utils.scheduler import start_scheduler


def run_etl():
    logger.info("Starting ETL pipeline...")

    try:
        logger.info("Extracting data from Alpha Vantage API...")
        extracted_data = extract()
        logger.info(f"Extracted data for {len(extracted_data)} symbols.")

        logger.info("Transforming extracted data...")
        transformer = StockDataTransformer()
        transformed_data = transformer.transform(extracted_data)
        logger.info("Data transformation complete.")

        logger.info("Loading data into database...")
        loader = StockDataLoader()
        loader.load(transformed_data)
        logger.info("Data successfully loaded into database.")

    except Exception as e:
        logger.exception(f"ETL pipeline failed: {e}")
    else:
        logger.info("ETL pipeline completed successfully.")
    finally:
        logger.info("ETL pipeline finished.")


def main():
    start_scheduler(run_etl)


if __name__ == "__main__":
    main()
