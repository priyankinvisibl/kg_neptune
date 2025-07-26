"""
Adapter for WikiPathway data from Enrichr
"""

from . import EnrichrAdapter
import re

class WikiPathwayAdapter(EnrichrAdapter):
    """Adapter for WikiPathway data from Enrichr"""
    
    def __init__(self, file_path=None):
        """Initialize the adapter"""
        super().__init__(file_path)
    
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        with open(self.file_path, 'r') as f:
            for line in f:
                items = line.strip().split('\t')
                name = items[0].rsplit(" ", 1)[0].replace("'", "").strip()
                match = re.search(r"(WP\d+)", items[0])
                if match:
                    pid = match.group(0)
                    yield (pid, "pathway", {"name": name})
                for gene in items[1:]:
                    gene = gene.strip()
                    if gene:
                        yield (gene, "gene", {})
    
    def get_edges(self):
        """Get edges for the knowledge graph"""
        with open(self.file_path, 'r') as f:
            for line in f:
                items = line.strip().split('\t')
                name = items[0].rsplit(" ", 1)[0].replace("'", "").strip()
                match = re.search(r"(WP\d+)", items[0])
                if match:
                    pid = match.group(0)
                    for gene in items[1:]:
                        gene = gene.strip()
                        if gene:
                            yield (f"interaction-{pid}-{gene}", pid, gene, "associated_with", {"pathway_name": name})
