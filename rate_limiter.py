import time
from threading import Event, Lock
from rich.console import Console

console = Console()

class GlobalRateLimiter:
    def __init__(self, pause_duration_minutes: int):
        self.pause_duration_minutes = pause_duration_minutes
        self.pause_event = Event()
        self.pause_event.set()
        self.lock = Lock()
        
    def trigger_global_pause(self, status_code: int, url: str):
        with self.lock:
            if self.pause_event.is_set():
                self.pause_event.clear()
                pause_seconds = self.pause_duration_minutes * 60
                console.print(f"[bold red]✗ Request failed with status {status_code} for {url}[/bold red]")
                console.print(f"[bold yellow]⏸ PAUSING ALL WORKERS for {self.pause_duration_minutes} minutes...[/bold yellow]")
                time.sleep(pause_seconds)
                console.print(f"[bold green]▶ RESUMING ALL WORKERS after {self.pause_duration_minutes} minute pause[/bold green]")
                self.pause_event.set()
    
    def wait_if_paused(self):
        self.pause_event.wait()
