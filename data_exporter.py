import json
import logging
from typing import Dict

logger = logging.getLogger(__name__)

class DataExporter:
    @staticmethod
    def export_to_json(data: Dict, filename: str) -> None:
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Data successfully exported to {filename}")
        except IOError as e:
            logger.error(f"Failed to export data to {filename}: {str(e)}")
            raise
