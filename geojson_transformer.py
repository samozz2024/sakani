import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

class GeoJSONTransformer:
    """Transforms collected data into GeoJSON standard format"""
    
    @staticmethod
    def transform_overview(overview_data: Dict) -> Dict:
        """Returns overview as regular JSON (not GeoJSON format)"""
        return overview_data
    
    @staticmethod
    def transform_mega_projects(mega_projects: List[Dict]) -> Dict:
        features = []
        
        for project in mega_projects:
            project_id = project.get("id", "")
            attributes = project.get("attributes", {})
            geo_shape = attributes.get("geo_shape")
            location = attributes.get("location", {})
            
            if geo_shape and isinstance(geo_shape, list) and len(geo_shape) > 0:
                geometry = {
                    "type": "Polygon",
                    "coordinates": geo_shape
                }
            elif location and location.get("latitude") and location.get("longitude"):
                geometry = {
                    "type": "Point",
                    "coordinates": [
                        float(location.get("longitude", 0)),
                        float(location.get("latitude", 0))
                    ]
                }
            else:
                geometry = None
            
            feature = {
                "type": "Feature",
                "id": f"mega_project_{project_id}",
                "geometry": geometry,
                "properties": attributes
            }
            
            features.append(feature)
        
        return {
            "type": "FeatureCollection",
            "features": features
        }
    
    @staticmethod
    def _create_project_feature(project: Dict) -> Dict:
        """Creates a GeoJSON feature for a project (includes demographics, insights, etc.)"""
        project_id = project.get("project_id", "")
        location = project.get("location", {})
        
        if location and location.get("latitude") and location.get("longitude"):
            geometry = {
                "type": "Point",
                "coordinates": [
                    float(location.get("longitude", 0)),
                    float(location.get("latitude", 0))
                ]
            }
        else:
            geometry = None
        
        # Include all project data except available_units (which become separate features)
        properties = {k: v for k, v in project.items() if k != "available_units"}
        
        return {
            "type": "Feature",
            "id": f"project_{project_id}",
            "geometry": geometry,
            "properties": properties
        }
    
    @staticmethod
    def _create_unit_feature(unit: Dict, project_id: str, project_location: Dict) -> Dict:
        """Creates a GeoJSON feature for a unit (links to parent project via project_id)"""
        unit_id = unit.get("id", "")
        unit_attributes = unit.get("attributes", {})
        unit_location = unit_attributes.get("location", {})
        
        # Use unit location if available, otherwise fallback to project location
        if unit_location and unit_location.get("latitude") and unit_location.get("longitude"):
            geometry = {
                "type": "Point",
                "coordinates": [
                    float(unit_location.get("longitude", 0)),
                    float(unit_location.get("latitude", 0))
                ]
            }
        elif project_location and project_location.get("latitude") and project_location.get("longitude"):
            geometry = {
                "type": "Point",
                "coordinates": [
                    float(project_location.get("longitude", 0)),
                    float(project_location.get("latitude", 0))
                ]
            }
        else:
            geometry = None
        
        properties = {
            "project_id": f"project_{project_id}",
            "id": unit_id,
            **unit_attributes,
            "unit_insights": unit.get("unit_insights", {}),
            "unit_project_trends": unit.get("unit_project_trends", []),
            "unit_transactions": unit.get("unit_transactions", [])
        }
        
        return {
            "type": "Feature",
            "id": f"unit_{unit_id}",
            "geometry": geometry,
            "properties": properties
        }
    
    @staticmethod
    def transform_projects(projects: List[Dict]) -> Dict:
        """Transforms projects into GeoJSON with both project and unit features"""
        project_features = []
        unit_features = []
        
        for project in projects:
            project_id = project.get("project_id", "")
            project_location = project.get("location", {})
            available_units = project.get("available_units", [])
            
            # Add project as a feature
            project_feature = GeoJSONTransformer._create_project_feature(project)
            project_features.append(project_feature)
            
            # Add each unit as a separate feature
            for unit in available_units:
                unit_feature = GeoJSONTransformer._create_unit_feature(
                    unit,
                    project_id,
                    project_location
                )
                unit_features.append(unit_feature)
        
        # Combine: all projects first, then all units
        return {
            "type": "FeatureCollection",
            "features": project_features + unit_features
        }
    
    @staticmethod
    def transform_market_units(market_units: List[Dict]) -> Dict:
        features = []
        
        for unit in market_units:
            unit_id = unit.get("unit_id", "")
            rega_ad_license = unit.get("rega_ad_license", {})
            location = rega_ad_license.get("location", {})
            
            if location and location.get("latitude") and location.get("longitude"):
                geometry = {
                    "type": "Point",
                    "coordinates": [
                        float(location.get("longitude", 0)),
                        float(location.get("latitude", 0))
                    ]
                }
            else:
                geometry = None
            
            properties = {k: v for k, v in unit.items()}
            
            feature = {
                "type": "Feature",
                "id": f"{unit_id}",
                "geometry": geometry,
                "properties": properties
            }
            
            features.append(feature)
        
        return {
            "type": "FeatureCollection",
            "features": features
        }
    
    @staticmethod
    def transform_all_data(data: Dict) -> Dict[str, Dict]:
        """Transforms all collected data into GeoJSON format for export"""
        transformed = {}
        
        if data.get("overview"):
            transformed["overview"] = GeoJSONTransformer.transform_overview(data)
            logger.info("Transformed overview to GeoJSON")
        
        if data.get("mega_projects"):
            transformed["mega_projects"] = GeoJSONTransformer.transform_mega_projects(data["mega_projects"])
            logger.info(f"Transformed {len(data['mega_projects'])} mega projects to GeoJSON")
        
        if data.get("projects_under_construction"):
            transformed["projects_under_construction"] = GeoJSONTransformer.transform_projects(data["projects_under_construction"])
            total_units = sum(len(p.get("available_units", [])) for p in data["projects_under_construction"])
            logger.info(f"Transformed {len(data['projects_under_construction'])} under construction projects with {total_units} units to GeoJSON")
        
        if data.get("projects_readymade"):
            transformed["projects_readymade"] = GeoJSONTransformer.transform_projects(data["projects_readymade"])
            total_units = sum(len(p.get("available_units", [])) for p in data["projects_readymade"])
            logger.info(f"Transformed {len(data['projects_readymade'])} readymade projects with {total_units} units to GeoJSON")
        
        if data.get("market_unit_buy"):
            transformed["market_unit_buy"] = GeoJSONTransformer.transform_market_units(data["market_unit_buy"])
            logger.info(f"Transformed {len(data['market_unit_buy'])} market units (buy) to GeoJSON")
        
        if data.get("market_lands_buy"):
            transformed["market_lands_buy"] = GeoJSONTransformer.transform_market_units(data["market_lands_buy"])
            logger.info(f"Transformed {len(data['market_lands_buy'])} market lands (buy) to GeoJSON")
        
        if data.get("market_unit_rent"):
            transformed["market_unit_rent"] = GeoJSONTransformer.transform_market_units(data["market_unit_rent"])
            logger.info(f"Transformed {len(data['market_unit_rent'])} market units (rent) to GeoJSON")
        
        return transformed
