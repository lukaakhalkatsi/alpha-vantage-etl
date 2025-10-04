import logging
from pathlib import Path
from config.settings import settings


log_dir = Path(settings.base_dir) / "logs"
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_dir / "etl.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("etl")
