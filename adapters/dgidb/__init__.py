"""
DGIdb adapters for processing drug-gene interaction data
"""

import logging
from abc import ABC, abstractmethod

class DgidbBaseAdapter(ABC):
    """Base adapter for DGIdb data processing"""
    
    def __init__(self, file_path=None):
        """Initialize the adapter"""
        self.file_path = file_path
        self.data = []
        self.logger = logging.getLogger(__name__)
        
        if file_path:
            self.parse_data()
    
    @abstractmethod
    def parse_data(self):
        """Parse data from the file"""
        pass
    
    @abstractmethod
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        pass
    
    @abstractmethod
    def get_edges(self):
        """Get edges for the knowledge graph"""
        pass
    
    def get_statistics(self):
        """Get statistics about the data"""
        return {
            "total_records": len(self.data)
        }
