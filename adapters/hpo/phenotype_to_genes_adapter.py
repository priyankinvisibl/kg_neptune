"""
Adapter for HPO phenotype_to_genes.txt file - Phenotype to Gene associations
"""

from . import HPOAdapter
import csv
import logging

class PhenotypeToGenesAdapter(HPOAdapter):
    """Adapter for HPO phenotype_to_genes.txt file"""
    
    def __init__(self, file_path=None):
        """Initialize the adapter"""
        self.genes = {}
        self.phenotypes = {}
        self.diseases = {}
        self.associations = []
        super().__init__(file_path)
    
    def parse_data(self):
        """Parse data from phenotype_to_genes.txt file"""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='\t')
                
                for row in reader:
                    hpo_id = row.get('hpo_id', '').strip()
                    hpo_name = row.get('hpo_name', '').strip()
                    ncbi_gene_id = row.get('ncbi_gene_id', '').strip()
                    gene_symbol = row.get('gene_symbol', '').strip()
                    disease_id = row.get('disease_id', '').strip()
                    
                    if hpo_id and gene_symbol:
                        # Store phenotype info
                        self.phenotypes[hpo_id] = {
                            'hpo_name': hpo_name,
                            'ncbi_gene_id': ncbi_gene_id,  # This might be confusing, but keeping as per schema
                            'data_source': 'HPO'
                        }
                        
                        # Store gene info
                        self.genes[gene_symbol] = {'data_source': 'HPO'}
                        
                        # Store disease info (if available)
                        if disease_id:
                            self.diseases[disease_id] = {'data_source': 'HPO'}
                        
                        # Store association
                        association = {
                            'hpo_id': hpo_id,
                            'gene_symbol': gene_symbol,
                            'disease_id': disease_id,
                            'ncbi_gene_id': ncbi_gene_id
                        }
                        self.associations.append(association)
                        self.data.append(row)
                        
        except Exception as e:
            self.logger.error(f"Error parsing phenotype_to_genes.txt file {self.file_path}: {e}")
    
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        # Phenotype nodes
        for hpo_id, phenotype_info in self.phenotypes.items():
            yield (hpo_id, "phenotypic feature", phenotype_info)
        
        # Gene nodes
        for gene_symbol, gene_info in self.genes.items():
            yield (gene_symbol, "gene", gene_info)
        
        # Disease nodes (basic - will be enriched by other adapters)
        for disease_id in self.diseases.keys():
            yield (disease_id, "disease", {})
    
    def get_edges(self):
        """Get edges for the knowledge graph"""
        for i, association in enumerate(self.associations):
            # Gene to Phenotype association
            gene_phenotype_edge_id = f"gene_phenotype_{i}"
            gene_source = association['gene_symbol']
            phenotype_target = association['hpo_id']
            
            gene_phenotype_properties = {
                'data_source': 'hpo'
            }
            
            # Add via_disease if disease_id is available
            if association.get('disease_id'):
                gene_phenotype_properties['via_disease'] = association['disease_id']
            
            yield (gene_phenotype_edge_id, gene_source, phenotype_target, 
                   "gene to phenotypic feature association", gene_phenotype_properties)
            
            # Phenotype to Disease association (if disease_id is available)
            if association.get('disease_id'):
                phenotype_disease_edge_id = f"phenotype_disease_{i}"
                phenotype_source = association['hpo_id']
                disease_target = association['disease_id']
                
                phenotype_disease_properties = {
                    'data_source': 'hpo',
                    'via_gene': association['gene_symbol']
                }
                
                yield (phenotype_disease_edge_id, phenotype_source, disease_target,
                       "phenotypic feature to disease association", phenotype_disease_properties)
    
    def get_statistics(self):
        """Get statistics about the data"""
        return {
            "total_records": len(self.data),
            "unique_phenotypes": len(self.phenotypes),
            "unique_genes": len(self.genes),
            "unique_diseases": len(self.diseases),
            "associations": len(self.associations)
        }
