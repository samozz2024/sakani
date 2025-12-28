import logging
from dotenv import load_dotenv
from rich.console import Console
from rich.logging import RichHandler

import configuration as config
from rate_limiter import GlobalRateLimiter
from http_client import HTTPClient
from api_client import SakaniAPIClient
from data_extractor import ProjectDataExtractor
from data_collector import ProjectDataCollector
from orchestrator import DataCollectionOrchestrator
from data_exporter import DataExporter

load_dotenv()

console = Console()

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[RichHandler(console=console, rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)

def main():
    try:
        rate_limiter = GlobalRateLimiter(config.pause_duration_minutes)
        http_client = HTTPClient(rate_limiter, config.speed_factor)
        api_client = SakaniAPIClient(http_client)
        extractor = ProjectDataExtractor(http_client.proxy_config, config.speed_factor)
        collector = ProjectDataCollector(api_client, extractor, config.max_workers, config.use_threading, config.max_retries, 
                                        config.unit_insights, config.unit_project_trends, config.unit_transactions)
        orchestrator = DataCollectionOrchestrator(api_client, collector, config)
        exporter = DataExporter()
        
        all_data = orchestrator.collect_all_data()
        
        if all_data:
            exporter.export_to_json(all_data, "sakani_data.json")
            mega_projects_count = len(all_data.get("mega_projects", []))
            projects_count = len(all_data.get("projects_under_construction", [])) + len(all_data.get("projects_readymade", []))
            market_units_count = len(all_data.get("market_unit_buy", [])) + len(all_data.get("market_lands_buy", [])) + len(all_data.get("market_unit_rent", []))
            logger.info(f"Successfully processed overview, {mega_projects_count} mega projects, {projects_count} projects, and {market_units_count} market items")
        else:
            logger.warning("No data to export")
    
    except Exception as e:
        logger.error(f"Fatal error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()
