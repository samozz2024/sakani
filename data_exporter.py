import json
import logging
import os
from typing import Dict
from geojson_transformer import GeoJSONTransformer

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

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
    
    @staticmethod
    def export_to_geojson_files(data: Dict, output_dir: str = "output") -> None:
        try:
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"Created/verified output directory: {output_dir}")
            
            transformed_data = GeoJSONTransformer.transform_all_data(data)
            
            file_mapping = {
                "overview": "overview.json",
                "mega_projects": "mega_projects.json",
                "projects_under_construction": "projects_under_construction.json",
                "projects_readymade": "projects_readymade.json",
                "market_unit_buy": "market_unit_buy.json",
                "market_lands_buy": "market_lands_buy.json",
                "market_unit_rent": "market_unit_rent.json"
            }
            
            exported_files = []
            for data_key, filename in file_mapping.items():
                if data_key in transformed_data:
                    filepath = os.path.join(output_dir, filename)
                    with open(filepath, "w", encoding="utf-8") as f:
                        json.dump(transformed_data[data_key], f, ensure_ascii=False, indent=2)
                    exported_files.append(filename)
                    logger.info(f"Exported {filename} with {len(transformed_data[data_key].get('features', []))} features")
            
            if exported_files:
                logger.info(f"Successfully exported {len(exported_files)} GeoJSON files to '{output_dir}' folder: {', '.join(exported_files)}")
            else:
                logger.warning("No data to export to GeoJSON files")
                
        except Exception as e:
            logger.error(f"Failed to export GeoJSON files: {str(e)}")
            raise
