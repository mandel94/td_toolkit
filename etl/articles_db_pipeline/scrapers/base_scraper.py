"""Base scraper class with Observer pattern for progress monitoring."""
import time
from abc import ABC, abstractmethod
from typing import List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import requests
from bs4 import BeautifulSoup
from loguru import logger


class ScrapingStatus(Enum):
    """Status of scraping operation."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    ERROR = "error"


@dataclass
class ScrapingProgress:
    """Progress information for scraping operations."""
    total_items: int = 0
    processed_items: int = 0
    successful_items: int = 0
    failed_items: int = 0
    current_batch: int = 0
    total_batches: int = 0
    status: ScrapingStatus = ScrapingStatus.IDLE
    current_url: Optional[str] = None
    error_message: Optional[str] = None
    start_time: Optional[datetime] = None
    last_update: datetime = field(default_factory=datetime.now)
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage."""
        if self.total_items == 0:
            return 0.0
        return (self.processed_items / self.total_items) * 100
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.processed_items == 0:
            return 0.0
        return (self.successful_items / self.processed_items) * 100
    
    def to_dict(self):
        """Convert to dictionary for logging."""
        return {
            'total': self.total_items,
            'processed': self.processed_items,
            'successful': self.successful_items,
            'failed': self.failed_items,
            'batch': f"{self.current_batch}/{self.total_batches}",
            'progress': f"{self.progress_percentage:.1f}%",
            'success_rate': f"{self.success_rate:.1f}%",
            'status': self.status.value,
            'current_url': self.current_url
        }


class ScraperObserver(ABC):
    """Observer interface for monitoring scraping progress."""
    
    @abstractmethod
    def on_progress_update(self, progress: ScrapingProgress) -> None:
        """Called when scraping progress is updated."""
        pass
    
    @abstractmethod
    def on_batch_complete(self, batch_number: int, batch_size: int) -> None:
        """Called when a batch is completed."""
        pass
    
    @abstractmethod
    def on_error(self, error: Exception, url: Optional[str] = None) -> None:
        """Called when an error occurs."""
        pass


class DefaultScraperObserver(ScraperObserver):
    """Default observer implementation with logging."""
    
    def on_progress_update(self, progress: ScrapingProgress) -> None:
        """Log progress update."""
        logger.info(f"Scraping progress: {progress.to_dict()}")
    
    def on_batch_complete(self, batch_number: int, batch_size: int) -> None:
        """Log batch completion."""
        logger.success(f"Batch {batch_number} completed: {batch_size} items processed")
    
    def on_error(self, error: Exception, url: Optional[str] = None) -> None:
        """Log error."""
        error_msg = f"Scraping error"
        if url:
            error_msg += f" at {url}"
        error_msg += f": {str(error)}"
        logger.error(error_msg)


class ScraperBase(ABC):
    """Base class for web scrapers with Observer pattern and rate limiting."""
    
    def __init__(
        self,
        base_url: str,
        delay_between_requests: float = 2.0,
        batch_size: int = 100,
        batch_pause_duration: int = 120,
        timeout: int = 30,
        max_retries: int = 3,
        user_agent: Optional[str] = None,
        verify_ssl: bool = True
    ):
        """Initialize base scraper.
        
        Args:
            base_url: Base URL of the website to scrape
            delay_between_requests: Delay in seconds between requests (default: 2.0)
            batch_size: Number of items to process before pausing (default: 100)
            batch_pause_duration: Pause duration in seconds after each batch (default: 120)
            timeout: Request timeout in seconds (default: 30)
            max_retries: Maximum number of retry attempts (default: 3)
            user_agent: Custom user agent string
        """
        self.base_url = base_url.rstrip('/')
        self.delay_between_requests = delay_between_requests
        self.batch_size = batch_size
        self.batch_pause_duration = batch_pause_duration
        self.timeout = timeout
        self.max_retries = max_retries
        self.verify_ssl = verify_ssl
        
        # Configure session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent or 
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Observer pattern
        self.observers: List[ScraperObserver] = []
        self.progress = ScrapingProgress()
        
        # Add default observer
        self.add_observer(DefaultScraperObserver())
        
        logger.info(f"Initialized {self.__class__.__name__} with base_url={base_url}")
    
    def add_observer(self, observer: ScraperObserver) -> None:
        """Add an observer to monitor scraping progress."""
        if observer not in self.observers:
            self.observers.append(observer)
    
    def remove_observer(self, observer: ScraperObserver) -> None:
        """Remove an observer."""
        if observer in self.observers:
            self.observers.remove(observer)
    
    def _notify_progress(self) -> None:
        """Notify all observers of progress update."""
        self.progress.last_update = datetime.now()
        for observer in self.observers:
            try:
                observer.on_progress_update(self.progress)
            except Exception as e:
                logger.warning(f"Observer notification failed: {str(e)}")
    
    def _notify_batch_complete(self, batch_number: int, batch_size: int) -> None:
        """Notify all observers of batch completion."""
        for observer in self.observers:
            try:
                observer.on_batch_complete(batch_number, batch_size)
            except Exception as e:
                logger.warning(f"Observer notification failed: {str(e)}")
    
    def _notify_error(self, error: Exception, url: Optional[str] = None) -> None:
        """Notify all observers of error."""
        for observer in self.observers:
            try:
                observer.on_error(error, url)
            except Exception as e:
                logger.warning(f"Observer notification failed: {str(e)}")
    
    def fetch_page(self, url: str, retries: int = 0) -> Optional[BeautifulSoup]:
        """Fetch a web page with retry logic.
        
        Args:
            url: URL to fetch
            retries: Current retry attempt
            
        Returns:
            BeautifulSoup object or None if failed
        """
        try:
            time.sleep(self.delay_between_requests)
            
            response = self.session.get(url, timeout=self.timeout, verify=self.verify_ssl)
            response.raise_for_status()
            
            return BeautifulSoup(response.text, 'html.parser')
            
        except requests.RequestException as e:
            if retries < self.max_retries:
                logger.warning(f"Request failed for {url}, retrying ({retries + 1}/{self.max_retries}): {str(e)}")
                time.sleep(self.delay_between_requests * (retries + 1))
                return self.fetch_page(url, retries + 1)
            else:
                logger.error(f"Failed to fetch {url} after {self.max_retries} retries: {str(e)}")
                self._notify_error(e, url)
                return None
    
    def process_in_batches(
        self,
        items: List[any],
        process_func: Callable,
        description: str = "Processing"
    ) -> List[any]:
        """Process items in batches with progress monitoring.
        
        Args:
            items: List of items to process
            process_func: Function to apply to each item
            description: Description for logging
            
        Returns:
            List of processed results
        """
        results = []
        total_items = len(items)
        total_batches = (total_items + self.batch_size - 1) // self.batch_size
        
        # Initialize progress
        self.progress = ScrapingProgress(
            total_items=total_items,
            total_batches=total_batches,
            status=ScrapingStatus.RUNNING,
            start_time=datetime.now()
        )
        self._notify_progress()
        
        for batch_num in range(total_batches):
            start_idx = batch_num * self.batch_size
            end_idx = min(start_idx + self.batch_size, total_items)
            batch = items[start_idx:end_idx]
            
            self.progress.current_batch = batch_num + 1
            
            logger.info(f"{description}: Batch {batch_num + 1}/{total_batches} ({len(batch)} items)")
            
            for item in batch:
                try:
                    result = process_func(item)
                    if result is not None:
                        results.append(result)
                        self.progress.successful_items += 1
                    else:
                        self.progress.failed_items += 1
                except Exception as e:
                    logger.error(f"Error processing item: {str(e)}")
                    self.progress.failed_items += 1
                    self._notify_error(e)
                finally:
                    self.progress.processed_items += 1
                    self._notify_progress()
            
            # Notify batch completion
            self._notify_batch_complete(batch_num + 1, len(batch))
            
            # Pause between batches (except for last batch)
            if batch_num < total_batches - 1:
                logger.info(f"Pausing for {self.batch_pause_duration} seconds before next batch...")
                self.progress.status = ScrapingStatus.PAUSED
                self._notify_progress()
                time.sleep(self.batch_pause_duration)
                self.progress.status = ScrapingStatus.RUNNING
        
        self.progress.status = ScrapingStatus.COMPLETED
        self._notify_progress()
        
        logger.success(f"{description} completed: {len(results)}/{total_items} items successful")
        
        return results
    
    @abstractmethod
    def scrape(self, *args, **kwargs):
        """Main scraping method to be implemented by subclasses."""
        pass
    
    def close(self):
        """Close the session and cleanup."""
        if self.session:
            self.session.close()
            logger.info("Scraper session closed")
