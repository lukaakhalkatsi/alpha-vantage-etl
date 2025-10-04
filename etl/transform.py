import pandas as pd
from typing import List, Tuple
from utils.logger import logger


class StockDataTransformer:
    """
    Transforms validated Alpha Vantage stock data into clean pandas DataFrames.
    """

    def _extract_time_series(self, raw_data):
        logger.debug("Extracting time series data...")
        return raw_data.get("Time Series (Daily)", {})

    def _to_dataframe(self, time_series) -> pd.DataFrame:
        if not time_series:
            logger.error("No time series data found.")
            raise ValueError("No time series data found.")

        logger.debug("Converting time series data to DataFrame.")
        df = pd.DataFrame.from_dict(time_series, orient="index").reset_index()
        df.rename(columns={"index": "date"}, inplace=True)

        df.rename(
            columns={
                "1. open": "open",
                "2. high": "high",
                "3. low": "low",
                "4. close": "close",
                "5. volume": "volume",
            },
            inplace=True,
        )

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        numeric_cols = ["open", "high", "low", "close", "volume"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

        logger.debug("DataFrame created and cleaned successfully.")
        return df

    def _add_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived columns like daily percentage change."""
        logger.debug("Adding derived feature: daily_change_percentage.")
        df["daily_change_percentage"] = ((df["close"] - df["open"]) / df["open"]) * 100
        df.sort_values("date", inplace=True)
        return df

    def transform(self, extracted_list) -> List[Tuple[str, pd.DataFrame]]:
        logger.info("Starting transformation process...")
        transformed_data = []

        for symbol, raw_data in extracted_list:
            logger.info(f"Transforming data for {symbol}...")
            time_series = self._extract_time_series(raw_data)

            try:
                df = self._to_dataframe(time_series)
                df = self._add_features(df)
                transformed_data.append((symbol, df))
                logger.info(f"Transformation complete for {symbol}.")
            except Exception as e:
                logger.exception(f"Error transforming data for {symbol}: {e}")

        logger.info("All transformations completed.")
        return transformed_data
