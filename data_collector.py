import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Optional, List, Dict
from rich.console import Console
from api_client import SakaniAPIClient
from data_extractor import ProjectDataExtractor

console = Console()
logger = logging.getLogger(__name__)

class ProjectDataCollector:
    def __init__(self, api_client: SakaniAPIClient, extractor: ProjectDataExtractor, 
                 max_workers: int, use_threading: bool, max_retries: int, 
                 unit_insights: bool, unit_project_trends: bool, unit_transactions: bool,
                 project_insight: bool, price_trends: bool, project_transactions: bool, demographics: bool):
        self.api_client = api_client
        self.extractor = extractor
        self.max_workers = max_workers
        self.use_threading = use_threading
        self.max_retries = max_retries
        self.unit_insights = unit_insights
        self.unit_project_trends = unit_project_trends
        self.unit_transactions = unit_transactions
        self.project_insight = project_insight
        self.price_trends = price_trends
        self.project_transactions = project_transactions
        self.demographics = demographics
        self.lock = Lock()
        self.processed_project_ids = set()
        self.processed_market_unit_ids = set()
    
    def _collect_single_project_data(self, project_id: str) -> Optional[Dict]:
        for attempt in range(self.max_retries):
            try:
                json_data = self.api_client.get_project_details(project_id)
                if not json_data:
                    if attempt < self.max_retries - 1:
                        console.print(f"[bold yellow]⚠[/bold yellow] No data for project {project_id}, retrying (attempt {attempt + 1}/{self.max_retries})...")
                        continue
                    return None
                
                extracted_data = self.extractor.extract_project_data(json_data)
                
                if self.price_trends:
                    extracted_data["price_trends"] = self.api_client.get_price_trends(project_id)
                else:
                    extracted_data["price_trends"] = []
                
                if self.demographics:
                    extracted_data["demographics"] = self.api_client.get_demographics(project_id)
                else:
                    extracted_data["demographics"] = {}
                
                if self.project_insight:
                    extracted_data["project_insight"] = self.api_client.get_project_insight(project_id)
                else:
                    extracted_data["project_insight"] = {}
                
                if self.project_transactions:
                    extracted_data["project_transactions"] = self.api_client.get_project_transactions(project_id)
                else:
                    extracted_data["project_transactions"] = []
                
                extracted_data["available_units"] = self._collect_available_units_with_details(project_id)
                extracted_data["unit_models"] = self.api_client.get_unit_models(project_id)
                
                return extracted_data
                
            except Exception as e:
                if attempt < self.max_retries - 1:
                    console.print(f"[bold yellow]⚠[/bold yellow] Error in project {project_id}, retrying (attempt {attempt + 1}/{self.max_retries})...")
                    continue
                else:
                    console.print(f"[bold red]✗[/bold red] Failed to collect project {project_id} after {self.max_retries} attempts")
                    return None
        
        return None
    
    def _collect_available_units_with_details(self, project_id: str) -> List[Dict]:
        available_units = self.api_client.get_available_units(project_id)
        
        if not available_units:
            return []
        
        def enrich_unit(unit: Dict) -> Dict:
            if not any([self.unit_insights, self.unit_project_trends, self.unit_transactions]):
                return {"unit_insights": {}, "unit_project_trends": [], "unit_transactions": [], **unit}
            
            unit_id = unit.get("id", "")
            if not unit_id:
                return {"unit_insights": {}, "unit_project_trends": [], "unit_transactions": [], **unit}
            
            for attempt in range(self.max_retries):
                try:
                    unit_insights = {}
                    unit_project_trends = []
                    unit_transactions = []
                    
                    if self.unit_insights:
                        logger.debug(f"Fetching insights for unit {unit_id}")
                        unit_insights = self.api_client.get_unit_insights(unit_id)
                    
                    if self.unit_project_trends:
                        logger.debug(f"Fetching project trends for unit {unit_id}")
                        unit_project_trends = self.api_client.get_unit_project_trends(unit_id)
                    
                    if self.unit_transactions:
                        logger.debug(f"Fetching transactions for unit {unit_id}")
                        unit_transactions = self.api_client.get_unit_transactions(unit_id)
                    
                    return {
                        "unit_insights": unit_insights,
                        "unit_project_trends": unit_project_trends,
                        "unit_transactions": unit_transactions,
                        **unit
                    }
                    
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        logger.warning(f"Error enriching unit {unit_id}, retrying (attempt {attempt + 1}/{self.max_retries})...")
                        continue
                    else:
                        logger.error(f"Failed to enrich unit {unit_id} after {self.max_retries} attempts: {str(e)}")
                        return {"unit_insights": {}, "unit_project_trends": [], "unit_transactions": [], **unit}
            
            return {"unit_insights": {}, "unit_project_trends": [], "unit_transactions": [], **unit}
        
        enriched_units = []
        if self.use_threading:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_unit = {executor.submit(enrich_unit, unit): unit for unit in available_units}
                
                for future in as_completed(future_to_unit):
                    try:
                        enriched_unit = future.result()
                        enriched_units.append(enriched_unit)
                    except Exception as e:
                        unit = future_to_unit[future]
                        logger.error(f"Error enriching unit {unit.get('id', 'unknown')}: {str(e)}")
                        enriched_units.append({"insights": {}, **unit})
        else:
            for unit in available_units:
                try:
                    enriched_unit = enrich_unit(unit)
                    enriched_units.append(enriched_unit)
                except Exception as e:
                    logger.error(f"Error enriching unit {unit.get('id', 'unknown')}: {str(e)}")
                    enriched_units.append({"insights": {}, **unit})
        
        return enriched_units
    
    def _collect_projects_batch(self, project_ids: List[str], output_data: Dict, data_key: str, category_name: str) -> None:
        if self.use_threading:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_project = {executor.submit(self._collect_single_project_data, pid): pid for pid in project_ids}
                
                completed = 0
                for future in as_completed(future_to_project):
                    project_id = future_to_project[future]
                    completed += 1
                    
                    try:
                        project_data = future.result()
                        if project_data:
                            with self.lock:
                                if project_id not in self.processed_project_ids:
                                    output_data[data_key].append(project_data)
                                    self.processed_project_ids.add(project_id)
                            
                            available_units_count = len(project_data.get("available_units", []))
                            unit_models_count = len(project_data.get("unit_models", []))
                            
                            console.print(
                                f"[bold green]✓[/bold green] [{completed}/{len(project_ids)}] "
                                f"[cyan]{category_name} Project {project_id}[/cyan] | "
                                f"[magenta]{available_units_count} available units[/magenta], "
                                f"[blue]{unit_models_count} unit models[/blue]"
                            )
                        else:
                            console.print(f"[bold yellow]⚠[/bold yellow] [{completed}/{len(project_ids)}] No data collected for {category_name.lower()} project {project_id}")
                    except Exception as e:
                        console.print(f"[bold red]✗[/bold red] [{completed}/{len(project_ids)}] Error processing {category_name.lower()} project {project_id}: {str(e)}")
        else:
            completed = 0
            for project_id in project_ids:
                completed += 1
                try:
                    project_data = self._collect_single_project_data(project_id)
                    if project_data:
                        if project_id not in self.processed_project_ids:
                            output_data[data_key].append(project_data)
                            self.processed_project_ids.add(project_id)
                        
                        available_units_count = len(project_data.get("available_units", []))
                        unit_models_count = len(project_data.get("unit_models", []))
                        
                        console.print(
                            f"[bold green]✓[/bold green] [{completed}/{len(project_ids)}] "
                            f"[cyan]{category_name} Project {project_id}[/cyan] | "
                            f"[magenta]{available_units_count} available units[/magenta], "
                            f"[blue]{unit_models_count} unit models[/blue]"
                        )
                    else:
                        console.print(f"[bold yellow]⚠[/bold yellow] [{completed}/{len(project_ids)}] No data collected for {category_name.lower()} project {project_id}")
                except Exception as e:
                    console.print(f"[bold red]✗[/bold red] [{completed}/{len(project_ids)}] Error processing {category_name.lower()} project {project_id}: {str(e)}")
        
        console.print(f"[bold green]✓[/bold green] Completed {len(output_data[data_key])} {category_name.lower()} projects")
    
    def _collect_single_market_unit(self, unit_id: str) -> Optional[Dict]:
        for attempt in range(self.max_retries):
            try:
                unit_data = self.api_client.get_market_unit_details(unit_id)
                if unit_data:
                    return {"unit_id": unit_id, **unit_data}
                elif attempt < self.max_retries - 1:
                    console.print(f"[bold yellow]⚠[/bold yellow] No data for market unit {unit_id}, retrying (attempt {attempt + 1}/{self.max_retries})...")
                    continue
                return None
            except Exception as e:
                if attempt < self.max_retries - 1:
                    console.print(f"[bold yellow]⚠[/bold yellow] Error in market unit {unit_id}, retrying (attempt {attempt + 1}/{self.max_retries})...")
                    continue
                else:
                    console.print(f"[bold red]✗[/bold red] Failed to collect market unit {unit_id} after {self.max_retries} attempts")
                    return None
        return None
    
    def _collect_market_units_batch(self, unit_ids: List[str], output_data: Dict, data_key: str, category_name: str) -> None:
        if self.use_threading:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_unit = {executor.submit(self._collect_single_market_unit, uid): uid for uid in unit_ids}
                
                completed = 0
                for future in as_completed(future_to_unit):
                    unit_id = future_to_unit[future]
                    completed += 1
                    
                    try:
                        unit_data = future.result()
                        if unit_data:
                            with self.lock:
                                if unit_id not in self.processed_market_unit_ids:
                                    output_data[data_key].append(unit_data)
                                    self.processed_market_unit_ids.add(unit_id)
                            
                            console.print(
                                f"[bold green]✓[/bold green] [{completed}/{len(unit_ids)}] "
                                f"[cyan]{category_name} Market Unit {unit_id}[/cyan]"
                            )
                        else:
                            console.print(f"[bold yellow]⚠[/bold yellow] [{completed}/{len(unit_ids)}] No data collected for {category_name.lower()} market unit {unit_id}")
                    except Exception as e:
                        console.print(f"[bold red]✗[/bold red] [{completed}/{len(unit_ids)}] Error processing {category_name.lower()} market unit {unit_id}: {str(e)}")
        else:
            completed = 0
            for unit_id in unit_ids:
                completed += 1
                unit_data = self._collect_single_market_unit(unit_id)
                if unit_data:
                    if unit_id not in self.processed_market_unit_ids:
                        output_data[data_key].append(unit_data)
                        self.processed_market_unit_ids.add(unit_id)
                    
                    console.print(
                        f"[bold green]✓[/bold green] [{completed}/{len(unit_ids)}] "
                        f"[cyan]{category_name} Market Unit {unit_id}[/cyan]"
                    )
                else:
                    console.print(f"[bold yellow]⚠[/bold yellow] [{completed}/{len(unit_ids)}] No data collected for {category_name.lower()} market unit {unit_id}")
        
        console.print(f"[bold green]✓[/bold green] Completed {len(output_data[data_key])} {category_name.lower()} market units")
