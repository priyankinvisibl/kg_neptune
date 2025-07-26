"""
Adapter for Gene Ontology Cellular Component data from Enrichr
"""

from . import EnrichrAdapter
import re

class CellComponentAdapter(EnrichrAdapter):
    """Adapter for Gene Ontology Cellular Component data from Enrichr"""
    
    def __init__(self, file_path=None):
        """Initialize the adapter"""
        super().__init__(file_path)
    
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        with open(self.file_path, 'r') as f:
            for line in f:
                items = line.strip().split('\t')
                name = items[0].rsplit(" ", 1)[0].replace("'", "").strip()
                match = re.search(r"(GO:\d+)", items[0])
                if match:
                    cid = match.group(0)
                    yield (cid, "cell_component", {"name": name})
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
                match = re.search(r"(GO:\d+)", items[0])
                if match:
                    cid = match.group(0)
                    for gene in items[1:]:
                        gene = gene.strip()
                        if gene:
                            yield (f"interaction-{cid}-{gene}", cid, gene, "located_in", {"component_name": name})
