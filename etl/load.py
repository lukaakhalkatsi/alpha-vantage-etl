from datetime import datetime
from typing import List, Tuple

import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

from config.settings import settings
from utils.logger import logger

Base = declarative_base()


class StockDailyData(Base):
    """SQLAlchemy model for stock daily data table."""
    __tablename__ = "stock_daily_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    open_price = Column(Float, nullable=False)
    high_price = Column(Float, nullable=False)
    low_price = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    daily_change_percentage = Column(Float)
    extraction_timestamp = Column(DateTime, default=datetime.utcnow)


class StockDataLoader:
    def __init__(self, db_path: str = settings.database_path):
        self.engine = create_engine(db_path)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        logger.info(f"Database initialized at {db_path}")

    def load(self, transformed_data: List[Tuple[str, pd.DataFrame]]):
        """Insert transformed data into the database."""
        session = self.Session()
        logger.info("Starting data load into the database...")

        try:
            for symbol, df in transformed_data:
                logger.info(f"Loading data for symbol: {symbol}")
                df_to_insert = df.rename(
                    columns={
                        "open": "open_price",
                        "high": "high_price",
                        "low": "low_price",
                        "close": "close_price",
                    }
                )

                df_to_insert["symbol"] = symbol
                df_to_insert["extraction_timestamp"] = datetime.utcnow()

                df_to_insert.to_sql(
                    name=StockDailyData.__tablename__,
                    con=self.engine,
                    if_exists="append",
                    index=False,
                )

                logger.info(f"Data for {symbol} inserted successfully.")

            session.commit()
            logger.info("All data loaded successfully.")
        except Exception as e:
            session.rollback()
            logger.exception(f"Error while loading data into database: {e}")
            raise
        finally:
            session.close()
            logger.debug("Database session closed.")
