"""
Base adapter classes and utilities for knowledge graph builders
"""

class KnowledgeGraphAdapter:
    """Base class for all knowledge graph adapters"""
    
    def __init__(self):
        """Initialize the adapter"""
        pass
    
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        raise NotImplementedError("Subclasses must implement get_nodes()")
    
    def get_edges(self):
        """Get edges for the knowledge graph"""
        raise NotImplementedError("Subclasses must implement get_edges()")
    
    def get_statistics(self):
        """Get statistics about the data"""
        return {}
