"""
Adapter for HPO phenotype.hpoa file - Disease to Phenotype associations
"""

from . import HPOAdapter
import csv
import logging

class PhenotypeHpoaAdapter(HPOAdapter):
    """Adapter for HPO phenotype.hpoa file"""
    
    def __init__(self, file_path=None):
        """Initialize the adapter"""
        self.diseases = {}
        self.phenotypes = {}
        self.associations = []
        super().__init__(file_path)
    
    def parse_data(self):
        """Parse data from phenotype.hpoa file"""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                # Skip comment lines
                lines = []
                for line in f:
                    if not line.startswith('#'):
                        lines.append(line)
                
                # Create a CSV reader from the non-comment lines
                import io
                csv_data = io.StringIO(''.join(lines))
                reader = csv.DictReader(csv_data, delimiter='\t')
                
                for row in reader:
                    database_id = row.get('database_id', '').strip()
                    disease_name = row.get('disease_name', '').strip()
                    hpo_id = row.get('hpo_id', '').strip()
                    reference = row.get('reference', '').strip()
                    evidence = row.get('evidence', '').strip()
                    onset = row.get('onset', '').strip()
                    frequency = row.get('frequency', '').strip()
                    sex = row.get('sex', '').strip()
                    
                    if database_id and disease_name and hpo_id:
                        # Store disease info
                        self.diseases[database_id] = {
                            'disease_name': disease_name,
                            'data_source': 'HPO'
                        }
                        
                        # Store phenotype info (we'll get the name from phenotype_to_genes.txt)
                        self.phenotypes[hpo_id] = {'data_source': 'HPO'}
                        
                        # Store association
                        association = {
                            'disease_id': database_id,
                            'hpo_id': hpo_id,
                            'reference': reference,
                            'evidence': evidence,
                            'onset': onset,
                            'frequency': frequency,
                            'sex': sex
                        }
                        self.associations.append(association)
                        self.data.append(row)
                        
        except Exception as e:
            self.logger.error(f"Error parsing phenotype.hpoa file {self.file_path}: {e}")
            import traceback
            traceback.print_exc()
    
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        # Disease nodes
        for disease_id, disease_info in self.diseases.items():
            yield (disease_id, "disease", disease_info)
        
        # Phenotype nodes removed - they should only be created by PhenotypeToGenesAdapter
        # which has the proper hpo_name data. Creating empty phenotype nodes here
        # causes conflicts and overwrites the proper names from PhenotypeToGenesAdapter.
    
    def get_edges(self):
        """Get edges for the knowledge graph"""
        for i, association in enumerate(self.associations):
            edge_id = f"disease_phenotype_{i}"
            source = association['disease_id']
            target = association['hpo_id']
            
            # Edge properties (filter out empty values)
            properties = {
                'data_source': 'hpo'
            }
            
            for prop in ['reference', 'evidence', 'onset', 'frequency', 'sex']:
                if association.get(prop):
                    properties[prop] = association[prop]
            
            yield (edge_id, source, target, "disease to phenotypic feature association", properties)
    
    def get_statistics(self):
        """Get statistics about the data"""
        return {
            "total_records": len(self.data),
            "unique_diseases": len(self.diseases),
            "unique_phenotypes": len(self.phenotypes),
            "associations": len(self.associations)
        }
