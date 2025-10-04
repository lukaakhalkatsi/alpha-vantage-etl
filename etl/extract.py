import os
import json
from pathlib import Path
from dotenv import load_dotenv
import requests
from pydantic import ValidationError

from config.settings import settings
from etl.models import AlphaVantageResponse
from utils.logger import logger

load_dotenv()
os.makedirs(settings.data_dir, exist_ok=True)


def fetch_and_validate(symbol: str):
    try:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": settings.alpha_vantage_api_key,
        }

        logger.info(f"Fetching data for {symbol}...")
        response = requests.get(settings.alpha_vantage_base_url, params=params)

        data = response.json()

        validated = AlphaVantageResponse(**data)
        logger.info(f"Validated data for {symbol}")
        return validated
    except ValidationError as e:
        logger.error(f"{e}")
        return None


def save_raw_data(symbol: str, data):
    Path(settings.data_dir).mkdir(parents=True, exist_ok=True)
    filename = f"{settings.data_dir}/{symbol}_2025-10-03.json"

    if hasattr(data, "model_dump"):
        data_to_save = data.model_dump()
    else:
        data_to_save = data

    with open(filename, "w") as f:
        json.dump(data_to_save, f, indent=4)

    logger.debug(f"Saved raw data for {symbol} → {filename}")


def extract():
    extracted_data = []

    for symbol in settings.symbols:
        data = fetch_and_validate(symbol)
        if not data:
            logger.warning(f"Skipping {symbol} due to missing or invalid data.")
            continue

        save_raw_data(symbol, data)
        extracted_data.append((symbol, data.model_dump()))

    logger.info(f"Extraction complete. Total symbols processed: {len(extracted_data)}")
    return extracted_data


if __name__ == "__main__":
    extract()
