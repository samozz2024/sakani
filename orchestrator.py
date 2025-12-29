from typing import Dict
from rich.console import Console
from api_client import SakaniAPIClient
from data_collector import ProjectDataCollector

console = Console()

class DataCollectionOrchestrator:
    """Orchestrates the entire data collection workflow"""
    def __init__(self, api_client: SakaniAPIClient, collector: ProjectDataCollector, config):
        self.api_client = api_client
        self.collector = collector
        self.config = config
    
    def collect_all_data(self) -> Dict:
        """Collects all enabled data categories based on configuration"""
        console.print("\n[bold cyan]═══ Starting Data Collection ═══[/bold cyan]\n")
        
        output_data = {
            "overview": {},
            "mega_projects": [],
            "projects_under_construction": [],
            "projects_readymade": [],
            "market_unit_buy": [],
            "market_lands_buy": [],
            "market_unit_rent": []
        }
        
        if self.config.overview:
            console.print("[bold]Fetching overview data...[/bold]")
            overview = self.api_client.get_overview()
            if overview:
                output_data["overview"] = overview
                console.print("[bold green]✓[/bold green] Successfully collected overview data")
            else:
                console.print("[bold yellow]⚠[/bold yellow] Failed to collect overview data")
        else:
            console.print("[bold yellow]⊘[/bold yellow] Overview collection disabled in configuration")
        
        if self.config.mega_projects:
            console.print("\n[bold]Fetching mega projects data...[/bold]")
            mega_projects = self.api_client.get_mega_projects()
            if mega_projects:
                if self.config.test_run:
                    mega_projects = mega_projects[:1]
                    console.print("[bold cyan]TEST MODE: Limited to first mega project[/bold cyan]")
                output_data["mega_projects"] = mega_projects
                console.print(f"[bold green]✓[/bold green] Successfully collected {len(mega_projects)} mega projects")
            else:
                console.print("[bold yellow]⚠[/bold yellow] Failed to collect mega projects data")
        else:
            console.print("\n[bold yellow]⊘[/bold yellow] Mega projects collection disabled in configuration")
        
        if self.config.projects_under_construction:
            console.print("\n[bold]Starting projects under construction data collection...[/bold]")
            under_construction_ids = self.api_client.get_project_ids(marketplace_purpose="buy", product_types="units_under_construction")
            if under_construction_ids:
                # Test mode limits to first item for fast testing
                if self.config.test_run:
                    under_construction_ids = under_construction_ids[:1]
                    console.print("[bold cyan]TEST MODE: Limited to first project[/bold cyan]")
                if self.config.use_threading:
                    console.print(f"[bold cyan]Processing {len(under_construction_ids)} under construction projects concurrently with {self.config.max_workers} workers[/bold cyan]\n")
                else:
                    console.print(f"[bold cyan]Processing {len(under_construction_ids)} under construction projects sequentially (threading disabled)[/bold cyan]\n")
                self.collector._collect_projects_batch(under_construction_ids, output_data, "projects_under_construction", "Under Construction")
            else:
                console.print("[bold yellow]⚠[/bold yellow] No under construction project IDs found")
        else:
            console.print("\n[bold yellow]⊘[/bold yellow] Projects under construction collection disabled in configuration")
        
        if self.config.projects_readymade:
            console.print("\n[bold]Starting readymade projects data collection...[/bold]")
            readymade_ids = self.api_client.get_project_ids(marketplace_purpose="buy", product_types="readymade_units")
            if readymade_ids:
                if self.config.test_run:
                    readymade_ids = readymade_ids[:1]
                    console.print("[bold cyan]TEST MODE: Limited to first project[/bold cyan]")
                if self.config.use_threading:
                    console.print(f"[bold cyan]Processing {len(readymade_ids)} readymade projects concurrently with {self.config.max_workers} workers[/bold cyan]\n")
                else:
                    console.print(f"[bold cyan]Processing {len(readymade_ids)} readymade projects sequentially (threading disabled)[/bold cyan]\n")
                self.collector._collect_projects_batch(readymade_ids, output_data, "projects_readymade", "Readymade")
            else:
                console.print("[bold yellow]⚠[/bold yellow] No readymade project IDs found")
        else:
            console.print("\n[bold yellow]⊘[/bold yellow] Readymade projects collection disabled in configuration")
        
        if self.config.market_unit_buy:
            console.print("\n[bold]Starting market unit buy data collection...[/bold]")
            market_unit_ids = self.api_client.get_market_unit_ids(marketplace_purpose="buy", product_types="readymade_units")
            if market_unit_ids:
                if self.config.test_run:
                    market_unit_ids = market_unit_ids[:1]
                    console.print("[bold cyan]TEST MODE: Limited to first unit[/bold cyan]")
                if self.config.use_threading:
                    console.print(f"[bold cyan]Processing {len(market_unit_ids)} market units concurrently with {self.config.max_workers} workers[/bold cyan]\n")
                else:
                    console.print(f"[bold cyan]Processing {len(market_unit_ids)} market units sequentially (threading disabled)[/bold cyan]\n")
                self.collector._collect_market_units_batch(market_unit_ids, output_data, "market_unit_buy", "Buy")
            else:
                console.print("[bold yellow]⚠[/bold yellow] No market unit IDs found")
        else:
            console.print("\n[bold yellow]⊘[/bold yellow] Market unit buy collection disabled in configuration")
        
        if self.config.market_lands_buy:
            console.print("\n[bold]Starting market lands buy data collection...[/bold]")
            market_lands_ids = self.api_client.get_market_unit_ids(marketplace_purpose="buy", product_types="lands")
            if market_lands_ids:
                if self.config.test_run:
                    market_lands_ids = market_lands_ids[:1]
                    console.print("[bold cyan]TEST MODE: Limited to first unit[/bold cyan]")
                if self.config.use_threading:
                    console.print(f"[bold cyan]Processing {len(market_lands_ids)} market lands concurrently with {self.config.max_workers} workers[/bold cyan]\n")
                else:
                    console.print(f"[bold cyan]Processing {len(market_lands_ids)} market lands sequentially (threading disabled)[/bold cyan]\n")
                self.collector._collect_market_units_batch(market_lands_ids, output_data, "market_lands_buy", "Lands")
            else:
                console.print("[bold yellow]⚠[/bold yellow] No market lands IDs found")
        else:
            console.print("\n[bold yellow]⊘[/bold yellow] Market lands buy collection disabled in configuration")
        
        if self.config.market_unit_rent:
            console.print("\n[bold]Starting market unit rent data collection...[/bold]")
            market_unit_rent_ids = self.api_client.get_market_unit_rent_ids()
            if market_unit_rent_ids:
                if self.config.test_run:
                    market_unit_rent_ids = market_unit_rent_ids[:1]
                    console.print("[bold cyan]TEST MODE: Limited to first unit[/bold cyan]")
                if self.config.use_threading:
                    console.print(f"[bold cyan]Processing {len(market_unit_rent_ids)} rental market units concurrently with {self.config.max_workers} workers[/bold cyan]\n")
                else:
                    console.print(f"[bold cyan]Processing {len(market_unit_rent_ids)} rental market units sequentially (threading disabled)[/bold cyan]\n")
                self.collector._collect_market_units_batch(market_unit_rent_ids, output_data, "market_unit_rent", "Rent")
            else:
                console.print("[bold yellow]⚠[/bold yellow] No rental market unit IDs found")
        else:
            console.print("\n[bold yellow]⊘[/bold yellow] Market unit rent collection disabled in configuration")
        
        under_construction_count = len(output_data['projects_under_construction'])
        readymade_count = len(output_data['projects_readymade'])
        buy_units_count = len(output_data['market_unit_buy'])
        lands_buy_count = len(output_data['market_lands_buy'])
        rent_units_count = len(output_data['market_unit_rent'])
        mega_projects_count = len(output_data['mega_projects'])
        
        under_construction_total_units = sum(
            len(project.get('available_units', [])) 
            for project in output_data['projects_under_construction']
        )
        readymade_total_units = sum(
            len(project.get('available_units', [])) 
            for project in output_data['projects_readymade']
        )
        
        console.print(f"\n[bold green]═══ Completed data collection ═══[/bold green]")
        console.print(f"[bold green]• Mega Projects: {mega_projects_count}[/bold green]")
        console.print(f"[bold green]• Projects Under Construction: {under_construction_count}[/bold green]")
        console.print(f"[bold green]  - Total units: {under_construction_total_units}[/bold green]")
        console.print(f"[bold green]• Readymade Projects: {readymade_count}[/bold green]")
        console.print(f"[bold green]  - Total units: {readymade_total_units}[/bold green]")
        console.print(f"[bold green]• Market Units (Buy): {buy_units_count}[/bold green]")
        console.print(f"[bold green]• Market Lands (Buy): {lands_buy_count}[/bold green]")
        console.print(f"[bold green]• Market Units (Rent): {rent_units_count}[/bold green]")
        console.print(f"[bold green]═══════════════════════════════[/bold green]\n")
        return output_data
