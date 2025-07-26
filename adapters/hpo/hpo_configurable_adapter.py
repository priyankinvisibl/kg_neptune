"""
Configurable adapter for HPO data
"""

from . import HPOAdapter
import yaml
import csv
import os
import logging

class HPOConfigurableAdapter(HPOAdapter):
    """Configurable adapter for HPO data"""
    
    def __init__(self, config_path=None):
        """Initialize the adapter"""
        super().__init__()
        self.config_path = config_path
        self.config = self._load_config()
        self.logger = logging.getLogger(__name__)
        
        # Data structures
        self.genes = {}
        self.diseases = {}
        self.phenotypes = {}
        self.gene_to_disease = []
        self.disease_to_phenotype = []
        self.gene_to_phenotype = []
    
    def _load_config(self):
        """Load configuration from YAML file"""
        if not self.config_path:
            self.logger.warning("No configuration file specified")
            return {}
        
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                return config
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            return {}
    
    def parse_all(self):
        """Parse all HPO data sources"""
        # Parse genes to disease
        if 'genes_to_disease' in self.config:
            self._parse_genes_to_disease()
        
        # Parse phenotype to genes
        if 'phenotype_to_genes' in self.config:
            self._parse_phenotype_to_genes()
        
        # Parse phenotype HPOA
        if 'phenotype_hpoa' in self.config:
            self._parse_phenotype_hpoa()
    
    def _parse_genes_to_disease(self):
        """Parse genes to disease data"""
        config = self.config.get('genes_to_disease', {})
        file_path = config.get('file_path')
        
        if not file_path:
            self.logger.warning("No file path specified for genes to disease")
            return
        
        try:
            # Get column mappings
            gene_id_col = config.get('gene_id_column', 0)
            gene_name_col = config.get('gene_name_column', 1)
            disease_id_col = config.get('disease_id_column', 2)
            disease_name_col = config.get('disease_name_column', 3)
            
            # Get property mappings
            node_properties = config.get('node_properties', [])
            edge_properties = config.get('edge_properties', [])
            
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter='\t')
                next(reader)  # Skip header
                
                for row in reader:
                    if len(row) <= max(gene_id_col, gene_name_col, disease_id_col, disease_name_col):
                        continue
                    
                    gene_id = row[gene_id_col]
                    gene_name = row[gene_name_col]
                    disease_id = row[disease_id_col]
                    disease_name = row[disease_name_col]
                    
                    # Add gene
                    if gene_id not in self.genes:
                        gene_props = {"name": gene_name}
                        
                        # Add additional properties
                        for prop in node_properties:
                            if isinstance(prop, dict):
                                prop_name = list(prop.keys())[0]
                                prop_col = prop[prop_name]
                                if prop_col < len(row):
                                    gene_props[prop_name] = row[prop_col]
                            elif isinstance(prop, str) and ":" in prop:
                                prop_name, prop_col = prop.split(":", 1)
                                try:
                                    prop_col = int(prop_col)
                                    if prop_col < len(row):
                                        gene_props[prop_name] = row[prop_col]
                                except ValueError:
                                    pass
                        
                        self.genes[gene_id] = gene_props
                    
                    # Add disease
                    if disease_id not in self.diseases:
                        self.diseases[disease_id] = {"name": disease_name}
                    
                    # Add gene to disease association
                    edge_props = {}
                    
                    # Add additional edge properties
                    for prop in edge_properties:
                        if isinstance(prop, dict):
                            prop_name = list(prop.keys())[0]
                            prop_col = prop[prop_name]
                            if prop_col < len(row):
                                edge_props[prop_name] = row[prop_col]
                        elif isinstance(prop, str) and ":" in prop:
                            prop_name, prop_col = prop.split(":", 1)
                            try:
                                prop_col = int(prop_col)
                                if prop_col < len(row):
                                    edge_props[prop_name] = row[prop_col]
                            except ValueError:
                                pass
                    
                    # Add data source
                    if 'global' in self.config and 'data_source' in self.config['global']:
                        edge_props['data_source'] = self.config['global']['data_source']
                    
                    self.gene_to_disease.append((gene_id, disease_id, edge_props))
            
            self.logger.info(f"Parsed {len(self.gene_to_disease)} gene to disease associations")
            
        except Exception as e:
            self.logger.error(f"Error parsing genes to disease: {e}")
            import traceback
            traceback.print_exc()
    
    def _parse_phenotype_to_genes(self):
        """Parse phenotype to genes data"""
        config = self.config.get('phenotype_to_genes', {})
        file_path = config.get('file_path')
        
        if not file_path:
            self.logger.warning("No file path specified for phenotype to genes")
            return
        
        try:
            # Get column mappings
            phenotype_id_col = config.get('phenotype_id_column', 0)
            phenotype_name_col = config.get('phenotype_name_column', 1)
            gene_id_col = config.get('gene_id_column', 2)
            gene_name_col = config.get('gene_name_column', 3)
            disease_id_col = config.get('disease_id_column', 4)
            disease_name_col = config.get('disease_name_column', 5)
            
            # Get property mappings
            node_properties = config.get('node_properties', [])
            edge_properties = config.get('edge_properties', [])
            special_properties = config.get('special_properties', {})
            
            # Get special property names
            gene_to_phenotype_via = special_properties.get('gene_to_phenotype_via', 'via_disease')
            phenotype_to_disease_via = special_properties.get('phenotype_to_disease_via', 'via_gene')
            
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter='\t')
                next(reader)  # Skip header
                
                for row in reader:
                    if len(row) <= max(phenotype_id_col, phenotype_name_col, gene_id_col, gene_name_col, disease_id_col, disease_name_col):
                        continue
                    
                    phenotype_id = row[phenotype_id_col]
                    phenotype_name = row[phenotype_name_col]
                    gene_id = row[gene_id_col]
                    gene_name = row[gene_name_col]
                    disease_id = row[disease_id_col]
                    disease_name = row[disease_name_col]
                    
                    # Add phenotype
                    if phenotype_id not in self.phenotypes:
                        phenotype_props = {"name": phenotype_name}
                        
                        # Add additional properties
                        for prop in node_properties:
                            if isinstance(prop, dict):
                                prop_name = list(prop.keys())[0]
                                prop_col = prop[prop_name]
                                if prop_col < len(row):
                                    phenotype_props[prop_name] = row[prop_col]
                            elif isinstance(prop, str) and ":" in prop:
                                prop_name, prop_col = prop.split(":", 1)
                                try:
                                    prop_col = int(prop_col)
                                    if prop_col < len(row):
                                        phenotype_props[prop_name] = row[prop_col]
                                except ValueError:
                                    pass
                        
                        self.phenotypes[phenotype_id] = phenotype_props
                    
                    # Add gene
                    if gene_id not in self.genes:
                        self.genes[gene_id] = {"name": gene_name}
                    
                    # Add disease
                    if disease_id not in self.diseases:
                        self.diseases[disease_id] = {"name": disease_name}
                    
                    # Add gene to phenotype association
                    gene_to_phenotype_props = {
                        gene_to_phenotype_via: disease_id
                    }
                    
                    # Add data source
                    if 'global' in self.config and 'data_source' in self.config['global']:
                        gene_to_phenotype_props['data_source'] = self.config['global']['data_source']
                    
                    self.gene_to_phenotype.append((gene_id, phenotype_id, gene_to_phenotype_props))
                    
                    # Add phenotype to disease association
                    phenotype_to_disease_props = {
                        phenotype_to_disease_via: gene_id
                    }
                    
                    # Add data source
                    if 'global' in self.config and 'data_source' in self.config['global']:
                        phenotype_to_disease_props['data_source'] = self.config['global']['data_source']
                    
                    self.disease_to_phenotype.append((disease_id, phenotype_id, phenotype_to_disease_props))
            
            self.logger.info(f"Parsed {len(self.gene_to_phenotype)} gene to phenotype associations")
            self.logger.info(f"Parsed {len(self.disease_to_phenotype)} disease to phenotype associations")
            
        except Exception as e:
            self.logger.error(f"Error parsing phenotype to genes: {e}")
            import traceback
            traceback.print_exc()
    
    def _parse_phenotype_hpoa(self):
        """Parse phenotype HPOA data"""
        config = self.config.get('phenotype_hpoa', {})
        file_path = config.get('file_path')
        
        if not file_path:
            self.logger.warning("No file path specified for phenotype HPOA")
            return
        
        try:
            # Get column mappings
            disease_id_col = config.get('disease_id_column', 0)
            disease_name_col = config.get('disease_name_column', 1)
            phenotype_id_col = config.get('phenotype_id_column', 3)
            phenotype_name_col = config.get('phenotype_name_column', 4)
            
            # Get property mappings
            node_properties = config.get('node_properties', [])
            edge_properties = config.get('edge_properties', [])
            
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter='\t')
                
                # Skip header lines
                for row in reader:
                    if row and row[0].startswith('#'):
                        continue
                    break
                
                for row in reader:
                    if len(row) <= max(disease_id_col, disease_name_col, phenotype_id_col, phenotype_name_col):
                        continue
                    
                    disease_id = row[disease_id_col]
                    disease_name = row[disease_name_col]
                    phenotype_id = row[phenotype_id_col]
                    phenotype_name = row[phenotype_name_col]
                    
                    # Add disease
                    if disease_id not in self.diseases:
                        disease_props = {"name": disease_name}
                        
                        # Add additional properties
                        for prop in node_properties:
                            if isinstance(prop, dict):
                                prop_name = list(prop.keys())[0]
                                prop_col = prop[prop_name]
                                if prop_col < len(row):
                                    disease_props[prop_name] = row[prop_col]
                            elif isinstance(prop, str) and ":" in prop:
                                prop_name, prop_col = prop.split(":", 1)
                                try:
                                    prop_col = int(prop_col)
                                    if prop_col < len(row):
                                        disease_props[prop_name] = row[prop_col]
                                except ValueError:
                                    pass
                        
                        self.diseases[disease_id] = disease_props
                    
                    # Add phenotype
                    if phenotype_id not in self.phenotypes:
                        self.phenotypes[phenotype_id] = {"name": phenotype_name}
                    
                    # Add disease to phenotype association
                    edge_props = {}
                    
                    # Add additional edge properties
                    for prop in edge_properties:
                        if isinstance(prop, dict):
                            prop_name = list(prop.keys())[0]
                            prop_col = prop[prop_name]
                            if prop_col < len(row):
                                edge_props[prop_name] = row[prop_col]
                        elif isinstance(prop, str) and ":" in prop:
                            prop_name, prop_col = prop.split(":", 1)
                            try:
                                prop_col = int(prop_col)
                                if prop_col < len(row):
                                    edge_props[prop_name] = row[prop_col]
                            except ValueError:
                                pass
                    
                    # Add data source
                    if 'global' in self.config and 'data_source' in self.config['global']:
                        edge_props['data_source'] = self.config['global']['data_source']
                    
                    self.disease_to_phenotype.append((disease_id, phenotype_id, edge_props))
            
            self.logger.info(f"Parsed {len(self.disease_to_phenotype)} disease to phenotype associations")
            
        except Exception as e:
            self.logger.error(f"Error parsing phenotype HPOA: {e}")
            import traceback
            traceback.print_exc()
    
    def get_nodes(self):
        """Get nodes for the knowledge graph"""
        # Generate gene nodes
        for gene_id, gene in self.genes.items():
            yield (
                gene_id,
                "gene",
                {
                    "name": gene.get("name", ""),
                    "id": gene_id,
                    **{k: v for k, v in gene.items() if k != "name"}
                }
            )
        
        # Generate disease nodes
        for disease_id, disease in self.diseases.items():
            yield (
                disease_id,
                "disease",
                {
                    "name": disease.get("name", ""),
                    "id": disease_id,
                    **{k: v for k, v in disease.items() if k != "name"}
                }
            )
        
        # Generate phenotype nodes
        for phenotype_id, phenotype in self.phenotypes.items():
            yield (
                phenotype_id,
                "phenotypic_feature",
                {
                    "name": phenotype.get("name", ""),
                    "id": phenotype_id,
                    **{k: v for k, v in phenotype.items() if k != "name"}
                }
            )
    
    def get_edges(self):
        """Get edges for the knowledge graph"""
        # Generate gene to disease edges
        for gene_id, disease_id, props in self.gene_to_disease:
            yield (
                gene_id,
                disease_id,
                "gene_to_disease_association",
                props
            )
        
        # Generate gene to phenotype edges
        for gene_id, phenotype_id, props in self.gene_to_phenotype:
            yield (
                gene_id,
                phenotype_id,
                "gene_to_phenotypic_feature_association",
                props
            )
        
        # Generate disease to phenotype edges
        for disease_id, phenotype_id, props in self.disease_to_phenotype:
            yield (
                disease_id,
                phenotype_id,
                "disease_to_phenotypic_feature_association",
                props
            )
