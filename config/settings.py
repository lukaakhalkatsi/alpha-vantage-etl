from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import List


class Settings(BaseSettings):
    alpha_vantage_api_key: str = Field(..., alias="ALPHA_VANTAGE_API_KEY")
    alpha_vantage_base_url: str = "https://www.alphavantage.co/query"
    companies: List[str] = ["AAPL", "GOOG", "MSFT"]

    database_path: str = Field("sqlite:///stock_data.db", alias="DATABASE_URL")
    data_dir: str = Field(..., alias="DATA_DIR")

    log_level: str = Field("INFO", alias="LOG_LEVEL")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )


settings = Settings()