import json
import logging
import os
import random
import time
from typing import Optional, Dict
from curl_cffi import requests
from curl_cffi.requests import RequestsError
from rich.console import Console
from rate_limiter import GlobalRateLimiter

console = Console()
logger = logging.getLogger(__name__)

class HTTPClient:
    def __init__(self, rate_limiter: GlobalRateLimiter, speed_factor: float):
        self.rate_limiter = rate_limiter
        self.speed_factor = speed_factor
        self.use_proxy = os.getenv("USE_PROXY", "False").lower() == "true"
        self.proxy_config = self._setup_proxy() if self.use_proxy else None
        
        if self.use_proxy:
            console.print("[bold green]Proxy enabled[/bold green]")
        else:
            console.print("[bold blue]Proxy disabled[/bold blue]")
    
    def _setup_proxy(self) -> Optional[Dict]:
        endpoint = os.getenv("PROXY_ENDPOINT")
        username = os.getenv("PROXY_USERNAME")
        password = os.getenv("PROXY_PASSWORD")
        
        if not all([endpoint, username, password]):
            logger.warning("Proxy configuration incomplete, proceeding without proxy")
            return None
        
        proxy_url = f"http://{username}:{password}@{endpoint}"
        return {"http": proxy_url}
    
    def make_request(self, url: str, params: Optional[Dict] = None, allow_404: bool = False) -> Optional[Dict]:
        self.rate_limiter.wait_if_paused()
        
        try:
            if self.proxy_config:
                response = requests.get(url, params=params, impersonate="chrome", proxies=self.proxy_config)
            else:
                response = requests.get(url, params=params, impersonate="chrome")
            
            delay = random.uniform(self.speed_factor - 0.02, self.speed_factor + 0.02)
            time.sleep(delay)
            
            if allow_404 and response.status_code == 404:
                return {}
            
            if response.status_code in [403, 429]:
                self.rate_limiter.trigger_global_pause(response.status_code, url)
                raise Exception(f"Request failed with status {response.status_code}")
            
            if response.status_code != 200:
                console.print(f"[bold red]✗ Request failed with status {response.status_code} for {url}[/bold red]")
                raise Exception(f"Request failed with status {response.status_code}")
            
            response.raise_for_status()
            return response.json()
        except RequestsError as e:
            console.print(f"[bold red]✗ Request error for {url}: {str(e)}[/bold red]")
            raise
        except json.JSONDecodeError as e:
            console.print(f"[bold red]✗ JSON decode error for {url}: {str(e)}[/bold red]")
            raise
