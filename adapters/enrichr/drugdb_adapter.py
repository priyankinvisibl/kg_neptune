"""
Adapter for Drug-Gene Interactions from Enrichr
"""

from . import EnrichrAdapter

class DrugDBAdapter(EnrichrAdapter):
    """Adapter for Drug-Gene Interactions from Enrichr"""
    
    def __init__(self, file_path=None):
        """Initialize the adapter"""
        super().__init__(file_path)
    
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        with open(self.file_path, 'r') as f:
            for line in f:
                items = line.strip().split('\t')
                drug = items[0].strip()
                yield (drug, "drug", {"data_source": "DGIdb"})
                for gene in items[1:]:
                    gene = gene.strip()
                    if gene:
                        yield (gene, "gene", {"data_source": "DGIdb"})
    
    def get_edges(self):
        """Get edges for the knowledge graph"""
        with open(self.file_path, 'r') as f:
            for line in f:
                items = line.strip().split('\t')
                drug = items[0].strip()
                for gene in items[1:]:
                    gene = gene.strip()
                    if gene:
                        yield (f"interaction-{drug}-{gene}", drug, gene, "targets", {"data_source": "DGIdb"})
