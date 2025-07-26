"""
Base adapter for CIViC data
"""

from ..import KnowledgeGraphAdapter
import logging
import os
import json

class CivicBaseAdapter(KnowledgeGraphAdapter):
    """Base adapter for CIViC data"""
    
    def __init__(self, data_dir=None):
        """Initialize the adapter"""
        super().__init__()
        self.data_dir = data_dir
        self.logger = logging.getLogger(__name__)
        
        # Data structures
        self.genes = {}
        self.variants = {}
        self.evidence_items = {}
        self.assertions = {}
        self.diseases = {}
        self.drugs = {}
        self.therapies = {}
        
        if data_dir:
            self.ensure_data_dir()
    
    def ensure_data_dir(self):
        """Ensure data directory exists"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def parse_data(self):
        """Parse data from files"""
        raise NotImplementedError("Subclasses must implement parse_data()")
    
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        raise NotImplementedError("Subclasses must implement get_nodes()")
    
    def get_edges(self):
        """Get edges for the knowledge graph"""
        raise NotImplementedError("Subclasses must implement get_edges()")
    
    def get_statistics(self):
        """Get statistics about the data"""
        return {
            "genes": len(self.genes),
            "variants": len(self.variants),
            "evidence_items": len(self.evidence_items),
            "assertions": len(self.assertions),
            "diseases": len(self.diseases),
            "drugs": len(self.drugs),
            "therapies": len(self.therapies)
        }
