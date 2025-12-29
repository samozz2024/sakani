import logging
import random
import time
from typing import Optional, List, Dict
from curl_cffi import requests

logger = logging.getLogger(__name__)

class ProjectDataExtractor:
    def __init__(self, proxy_config: Optional[Dict], speed_factor: float):
        self.proxy_config = proxy_config
        self.speed_factor = speed_factor
    
    def fetch_geojson_features(self, url: str) -> Optional[List]:
        if not url:
            return None
        try:
            delay = random.uniform(self.speed_factor + 0.02, self.speed_factor + 0.02)
            time.sleep(delay)
            
            if self.proxy_config:
                response = requests.get(url, impersonate="chrome", proxies=self.proxy_config)
            else:
                response = requests.get(url, impersonate="chrome")
            
            if response.status_code == 200:
                data = response.json()
                return data.get("features", None)
        except Exception as e:
            logger.debug(f"Failed to fetch GeoJSON from {url}: {str(e)}")
        return None
    
    def extract_media(self, media: Dict) -> Dict:
        geo_map_url = media.get("geo_map", {}).get("attributes", {}).get("url", "")
        geo_map_polygons_url = media.get("geo_map_polygons", {}).get("attributes", {}).get("url", "")
        
        return {
            "banner": media.get("banner", {}).get("attributes", {}).get("url", ""),
            "gallery": [
                item.get("attributes", {}).get("url", "")
                for item in media.get("gallery", [])
            ],
            "geo_map": geo_map_url if geo_map_url else None,
            "brochure": media.get("brochure", {}).get("attributes", {}).get("url", ""),
            "master_plan": media.get("master_plan", {}).get("attributes", {}).get("url", ""),
            "geo_map_polygons": geo_map_polygons_url if geo_map_polygons_url else None,
        }
    
    @staticmethod
    def extract_project_unit_types(included: List[Dict]) -> List[Dict]:
        return [
            item.get("attributes", {})
            for i, item in enumerate(included)
            if i != 0
        ]
    
    def extract_project_data(self, json_data: Dict) -> Dict:
        project_data = json_data.get("data", {})
        attributes = project_data.get("attributes", {})
        
        extracted_data = {
            "project_id": project_data.get("id", ""),
            "project_code": attributes.get("code", ""),
            "project_name": attributes.get("name", ""),
            "publish_date": attributes.get("publish_date", ""),
            "region_obj": attributes.get("region_obj", ""),
            "city_obj": attributes.get("city_obj", ""),
            "phase": attributes.get("phase", ""),
            "status": attributes.get("status", ""),
            "bookable": attributes.get("bookable", ""),
            "location": attributes.get("location", ""),
            "units_statistic_data": attributes.get("units_statistic_data", ""),
            "subsidize_level": attributes.get("subsidize_level", ""),
            "price_starting_at": attributes.get("price_starting_at", ""),
            "realtime_available_units_count": attributes.get("realtime_available_units_count", ""),
            "can_request_conveyance_on_project": attributes.get("can_request_conveyance_on_project", ""),
            "booking_fee": attributes.get("booking_fee", ""),
            "booking_fee_setting_snapshot_values": attributes.get("booking_fee_setting_snapshot_values", ""),
            "automatic_cancel_delay_in_days_value": attributes.get("automatic_cancel_delay_in_days_value", ""),
            "azm_item_status": attributes.get("azm_item_status", ""),
            "completion_percentage": attributes.get("completion_percentage", ""),
            "completion_percentage_updated_at": attributes.get("completion_percentage_updated_at", ""),
            "units_available_soon": attributes.get("units_available_soon", ""),
            "extend_pq_fee": attributes.get("extend_pq_fee", ""),
            "extend_pq_day": attributes.get("extend_pq_day", ""),
            "maximum_booking_per_non_beneficiary": attributes.get("maximum_booking_per_non_beneficiary", ""),
            "auto_cancellation": attributes.get("auto_cancellation", ""),
            "booking_fee_payment_period": attributes.get("booking_fee_payment_period", ""),
            "unit_release_status": attributes.get("unit_release_status", ""),
            "mega_project_id": attributes.get("mega_project_id", ""),
            "nhc_related": attributes.get("nhc_related", ""),
            "sale_contract_period_in_hours": attributes.get("sale_contract_period_in_hours", ""),
            "post_sale_contract_period_actions": attributes.get("post_sale_contract_period_actions", ""),
            "broker_allowed_channels": attributes.get("broker_allowed_channels", ""),
            "allow_individual_brokers": attributes.get("allow_individual_brokers", ""),
            "developer_name": attributes.get("developer_name", ""),
            "discount_enabled": attributes.get("discount_enabled", ""),
            "media": self.extract_media(attributes.get("media", {})),
            "project_unit_types": ProjectDataExtractor.extract_project_unit_types(json_data.get("included", [])),
        }
        
        return extracted_data
