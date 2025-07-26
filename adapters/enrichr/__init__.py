"""
Base adapter for Enrichr data
"""

from ..import KnowledgeGraphAdapter
import csv
import logging

class EnrichrAdapter(KnowledgeGraphAdapter):
    """Base adapter for Enrichr data"""
    
    def __init__(self, file_path=None):
        """Initialize the adapter"""
        super().__init__()
        self.file_path = file_path
        self.data = {}
        self.logger = logging.getLogger(__name__)
        
        if file_path:
            self.parse_data()
    
    def parse_data(self):
        """Parse data from file"""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    
                    parts = line.split('\t')
                    if len(parts) >= 1:
                        term = parts[0]
                        # Filter out empty gene entries
                        genes = [g.strip() for g in parts[1:] if g.strip()]
                        self.data[term] = genes
        except Exception as e:
            self.logger.error(f"Error parsing data from {self.file_path}: {e}")
    
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        raise NotImplementedError("Subclasses must implement get_nodes()")
    
    def get_edges(self):
        """Get edges for the knowledge graph"""
        raise NotImplementedError("Subclasses must implement get_edges()")
    
    def get_statistics(self):
        """Get statistics about the data"""
        return {
            "terms": len(self.data),
            "genes": len(set(gene for genes in self.data.values() for gene in genes)),
            "associations": sum(len(genes) for genes in self.data.values())
        }
