"""
Base adapter for HPO data
"""

from ..import KnowledgeGraphAdapter
import logging

class HPOAdapter(KnowledgeGraphAdapter):
    """Base adapter for HPO data"""
    
    def __init__(self, file_path=None):
        """Initialize the adapter"""
        super().__init__()
        self.file_path = file_path
        self.logger = logging.getLogger(__name__)
        
        # Data structures
        self.genes = {}
        self.diseases = {}
        self.phenotypes = {}
        self.gene_to_disease = []
        self.disease_to_phenotype = []
        self.gene_to_phenotype = []
        
        if file_path:
            self.parse_data()
    
    def parse_data(self):
        """Parse data from file"""
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
            "diseases": len(self.diseases),
            "phenotypes": len(self.phenotypes),
            "edges": len(self.gene_to_disease) + len(self.disease_to_phenotype) + len(self.gene_to_phenotype)
        }
