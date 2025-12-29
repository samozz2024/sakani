import logging
from typing import Optional, List, Dict
from http_client import HTTPClient

logger = logging.getLogger(__name__)

class SakaniAPIClient:
    def __init__(self, http_client: HTTPClient):
        self.http_client = http_client
    
    def get_overview(self) -> Optional[Dict]:
        url = "https://sakani.sa/analyticCollector/embedded_insights/overview"
        data = self.http_client.make_request(url)
        return data.get("data", {}).get("attributes", {}) if data else {}
    
    def get_project_ids(self, marketplace_purpose: str = "buy", product_types: Optional[str] = "units_under_construction") -> List[str]:
        """Fetches all project IDs for a given category"""
        url = "https://sakani.sa/marketplaceApi/search/v3/location"
        params = {
            "filter[marketplace_purpose]": marketplace_purpose,
            "filter[mode]": "maps",
        }
        
        if product_types:
            params["filter[product_types]"] = product_types
        
        data = self.http_client.make_request(url, params)
        
        category = product_types if product_types else marketplace_purpose
        
        if not data:
            logger.warning(f"Failed to fetch project IDs for {category}")
            return []
        
        project_ids = []
        for item in data.get("data", []):
            # Filter only projects (not individual units)
            if item.get("attributes", {}).get("resource_type") == "projects":
                project_ids.append(item["id"][8:])  # Remove 'project_' prefix
        
        logger.info(f"Found {len(project_ids)} projects for {category}")
        return project_ids
    
    def get_project_details(self, project_id: str) -> Optional[Dict]:
        url = f"https://sakani.sa/mainIntermediaryApi/v4/projects/{project_id}?include=amenities,projects_amenities,developer,project_unit_types"
        return self.http_client.make_request(url)
    
    def get_price_trends(self, project_id: str, months: int = 12) -> List[Dict]:
        url = "https://sakani.sa/analyticCollector/compare_insights/price_trends"
        params = {
            "filter[project_id]": project_id,
            "filter[months_back_trend]": months,
        }
        data = self.http_client.make_request(url, params)
        return data.get("data", {}).get("attributes", {}).get("price_trends_data", []) if data else []
    
    def get_demographics(self, project_id: str) -> Dict:
        url = "https://sakani.sa/analyticCollector/compare_insights/demographic_overview"
        params = {"filter[project_id]": project_id}
        data = self.http_client.make_request(url, params)
        return data.get("data", {}).get("attributes", {}) if data else {}
    
    def get_project_insight(self, project_id: str) -> Dict:
        url = f"https://sakani.sa/analyticCollector/embedded_insights/projects/{project_id}"
        data = self.http_client.make_request(url)
        return data.get("data", {}).get("attributes", {}) if data else {}
    
    def get_project_transactions(self, project_id: str) -> List[Dict]:
        url = "https://sakani.sa/analyticCollector/compare_insights/project_transactions"
        params = {"filter[project_id]": project_id}
        data = self.http_client.make_request(url, params)
        return data.get("data", {}).get("attributes", {}).get("project_transactions_data", []) if data else []
    
    def get_available_units(self, project_id: str) -> List[Dict]:
        url = f"https://sakani.sa/marketplaceApi/search/v1/projects/{project_id}/available-units"
        data = self.http_client.make_request(url)
        return data.get("data", []) if data else []
    
    def get_unit_models(self, project_id: str) -> List[Dict]:
        url = "https://sakani.sa/mainIntermediaryApi/v4/unit_models"
        params = {"filter[project_id]": project_id}
        data = self.http_client.make_request(url, params)
        return data.get("data", []) if data else []
    
    def get_unit_insights(self, unit_id: str) -> Dict:
        url = f"https://sakani.sa/analyticCollector/embedded_insights/units/{unit_id}"
        data = self.http_client.make_request(url, allow_404=True)
        return data.get("data", {}).get("attributes", {}) if data else {}
    
    def get_unit_project_trends(self, unit_id: str, months: int = 12) -> List[Dict]:
        url = "https://sakani.sa/analyticCollector/compare_insights/unit_project_trends"
        params = {
            "filter[unit_id]": unit_id,
            "filter[months_back_trend]": months,
        }
        data = self.http_client.make_request(url, params)
        return data.get("data", {}).get("attributes", {}).get("unit_project_trends_data", []) if data else []
    
    def get_unit_transactions(self, unit_id: str) -> List[Dict]:
        url = "https://sakani.sa/analyticCollector/compare_insights/unit_transactions"
        params = {"filter[unit_id]": unit_id}
        data = self.http_client.make_request(url, params)
        return data.get("data", {}).get("attributes", {}).get("unit_transactions_data", []) if data else []
    
    def get_market_unit_ids(self, marketplace_purpose: str = "buy", product_types: Optional[str] = "readymade_units") -> List[str]:
        """Fetches market unit IDs (individual units not part of projects)"""
        url = "https://sakani.sa/marketplaceApi/search/v3/location"
        params = {
            "filter[marketplace_purpose]": marketplace_purpose,
            "filter[mode]": "maps",
        }
        
        if product_types:
            params["filter[product_types]"] = product_types
        
        data = self.http_client.make_request(url, params)
        if not data:
            logger.warning(f"Failed to fetch market unit IDs for {product_types}")
            return []
        
        market_unit_ids = []
        for item in data.get("data", []):
            if "market_unit" in item.get("id", ""):
                market_unit_ids.append(item["id"][12:])
        
        logger.info(f"Found {len(market_unit_ids)} market units for {product_types}")
        return market_unit_ids
    
    def get_market_unit_rent_ids(self) -> List[str]:
        """Fetches rental market unit IDs"""
        url = "https://sakani.sa/marketplaceApi/search/v2/location"
        params = {
            "filter[marketplace_purpose]": "rent",
            "filter[mode]": "maps",
        }
        
        data = self.http_client.make_request(url, params)
        if not data:
            logger.warning("Failed to fetch rental market unit IDs")
            return []
        
        market_unit_ids = []
        for item in data.get("data", []):
            if "market_unit" in item.get("id", ""):
                market_unit_ids.append(item["id"][12:])
        
        logger.info(f"Found {len(market_unit_ids)} rental market units")
        return market_unit_ids
    
    def get_market_unit_details(self, unit_id: str) -> Optional[Dict]:
        url = f"https://sakani.sa/marketUnitsApi/v6/market_units/{unit_id}"
        data = self.http_client.make_request(url)
        return data.get("data", {}).get("attributes", {}) if data else {}
    
    def get_mega_projects(self) -> List[Dict]:
        url = "https://sakani.sa/marketplaceApi/search/v2/mega-projects?page%5Bsize%5D=100&page%5Bnumber%5D=1"
        data = self.http_client.make_request(url)
        if not data:
            logger.warning("Failed to fetch mega projects")
            return []
        
        mega_projects = data.get("data", [])
        logger.info(f"Found {len(mega_projects)} mega projects")
        return mega_projects
