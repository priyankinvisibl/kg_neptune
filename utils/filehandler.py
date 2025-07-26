"""
File handler for downloading and managing files
"""

import os
import requests
import logging
from pathlib import Path

class FileHandler:
    """File handler for downloading and managing files"""
    
    def __init__(self, biocypher_instance=None):
        """Initialize the file handler"""
        self.biocypher = biocypher_instance
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # Configure logging
        self.logger = logging.getLogger(__name__)
    
    def download_file(self, name, url, force=False):
        """
        Download a file from a URL and save it to the data directory
        
        Args:
            name: Name of the file
            url: URL to download from
            force: Whether to force download even if file exists
            
        Returns:
            Path to the downloaded file
        """
        file_path = self.data_dir / f"{name}.txt"
        
        # Check if file exists
        if file_path.exists() and not force:
            self.logger.info(f"File {file_path} already exists, skipping download")
            return file_path
        
        # Download file
        self.logger.info(f"Downloading {name} from {url}")
        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            
            with open(file_path, "wb") as f:
                f.write(response.content)
            
            self.logger.info(f"Downloaded {name} to {file_path}")
            return file_path
        
        except Exception as e:
            self.logger.error(f"Error downloading {name}: {e}")
            return None
    
    def read_file(self, file_path):
        """
        Read a file and return its contents
        
        Args:
            file_path: Path to the file
            
        Returns:
            Contents of the file
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            self.logger.error(f"Error reading {file_path}: {e}")
            return None
