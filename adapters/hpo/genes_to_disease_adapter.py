"""
Adapter for HPO genes_to_disease.txt file - Gene to Disease associations
"""

from . import HPOAdapter
import csv
import logging

class GenesToDiseaseAdapter(HPOAdapter):
    """Adapter for HPO genes_to_disease.txt file"""
    
    def __init__(self, file_path=None):
        """Initialize the adapter"""
        self.genes = {}
        self.diseases = {}
        self.associations = []
        super().__init__(file_path)
    
    def parse_data(self):
        """Parse data from genes_to_disease.txt file"""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='\t')
                
                for row in reader:
                    ncbi_gene_id = row.get('ncbi_gene_id', '').strip()
                    gene_symbol = row.get('gene_symbol', '').strip()
                    association_type = row.get('association_type', '').strip()
                    disease_id = row.get('disease_id', '').strip()
                    source = row.get('source', '').strip()
                    
                    if gene_symbol and disease_id:
                        # Store gene info
                        self.genes[gene_symbol] = {}
                        
                        # Store disease info
                        self.diseases[disease_id] = {}
                        
                        # Store association
                        association = {
                            'gene_symbol': gene_symbol,
                            'disease_id': disease_id,
                            'association_type': association_type,
                            'source': source,
                            'ncbi_gene_id': ncbi_gene_id
                        }
                        self.associations.append(association)
                        self.data.append(row)
                        
        except Exception as e:
            self.logger.error(f"Error parsing genes_to_disease.txt file {self.file_path}: {e}")
    
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        # Gene nodes
        for gene_symbol, gene_info in self.genes.items():
            yield (gene_symbol, "gene", gene_info)
        
        # Disease nodes removed - they should only be created by PhenotypeHpoaAdapter
        # which has the proper disease_name data. Creating empty disease nodes here
        # causes conflicts and overwrites the proper names from PhenotypeHpoaAdapter.
    
    def get_edges(self):
        """Get edges for the knowledge graph"""
        for i, association in enumerate(self.associations):
            edge_id = f"gene_disease_{i}"
            source = association['gene_symbol']
            target = association['disease_id']
            
            # Edge properties (filter out empty values)
            properties = {
                'data_source': 'hpo'
            }
            
            for prop in ['association_type', 'source']:
                if association.get(prop):
                    properties[prop] = association[prop]
            
            yield (edge_id, source, target, "gene to disease association", properties)
    
    def get_statistics(self):
        """Get statistics about the data"""
        return {
            "total_records": len(self.data),
            "unique_genes": len(self.genes),
            "unique_diseases": len(self.diseases),
            "associations": len(self.associations)
        }
