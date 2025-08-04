"""
DGIdb Adapter for processing drug-gene interaction data
Follows the same pattern as CIViC and other adapters
"""

from . import DgidbBaseAdapter
import os
import csv
import logging
import requests
from datetime import datetime

class DgidbAdapter(DgidbBaseAdapter):
    """Adapter for DGIdb data with URL-based downloads"""
    
    def __init__(self, data_dir=None):
        """Initialize the adapter"""
        super().__init__()
        self.data_dir = data_dir or "/app/data"
        self.logger = logging.getLogger(__name__)
        
        # File paths for TSV files (downloaded from URLs)
        self.interactions_file = os.path.join(self.data_dir, "dgidb_interactions.tsv")
        self.genes_file = os.path.join(self.data_dir, "dgidb_genes.tsv")
        self.drugs_file = os.path.join(self.data_dir, "dgidb_drugs.tsv")
        self.categories_file = os.path.join(self.data_dir, "dgidb_categories.tsv")
        
        # Data structures for nodes
        self.genes = {}          # gene_name -> gene_data
        self.drugs = {}          # drug_name -> drug_data
        self.categories = {}     # category_name -> category_data
        self.interactions = []   # list of interaction records

    def download_data(self, config=None):
        """Download data from DGIdb URLs"""
        self.logger.info("Downloading DGIdb data files...")
        
        if not config or 'datasets' not in config or 'dgidb' not in config['datasets']:
            self.logger.error("No DGIdb configuration found")
            return
        
        dgidb_config = config['datasets']['dgidb']
        
        # URLs for DGIdb data files
        urls = {
            self.interactions_file: dgidb_config['interactions']['url'],
            self.genes_file: dgidb_config['genes']['url'],
            self.drugs_file: dgidb_config['drugs']['url'],
            self.categories_file: dgidb_config['categories']['url']
        }
        
        for file_path, url in urls.items():
            file_name = os.path.basename(file_path).replace('dgidb_', '')
            self.logger.info(f"Getting DGIdb {file_name} data...")
            
            try:
                response = requests.get(url, timeout=300)
                response.raise_for_status()
                
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                with open(file_path, "w", encoding='utf-8') as f:
                    f.write(response.text)
                
                self.logger.info(f"✅ {file_name}: {file_path}")
                
            except Exception as e:
                self.logger.error(f"❌ Failed to download {file_name}: {e}")
                raise

    def parse_data(self):
        """Parse data from DGIdb TSV files"""
        self.logger.info("Parsing DGIdb data from TSV files")
        
        # Parse in order: Genes → Drugs → Categories → Interactions
        self._parse_genes()
        self._parse_drugs()
        self._parse_categories()
        self._parse_interactions()
        
        self.logger.info("DGIdb data parsing complete")
        self._log_statistics()

    def _parse_genes(self):
        """Parse genes.tsv file"""
        self.logger.info("Parsing genes...")
        
        if not os.path.exists(self.genes_file):
            self.logger.warning(f"Genes file not found: {self.genes_file}")
            return
        
        with open(self.genes_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                # Use gene_name as primary, fallback to gene_claim_name
                gene_name = row.get("gene_name", "").strip()
                gene_claim_name = row.get("gene_claim_name", "").strip()
                
                if not gene_name or gene_name == 'NULL':
                    gene_name = gene_claim_name
                
                if gene_name and gene_name != 'NULL':
                    # Clean gene name for ID
                    gene_id = gene_name.replace(' ', '_').replace('-', '_')
                    
                    self.genes[gene_id] = {
                        "id": gene_id,
                        "gene_name": gene_name,
                        "gene_claim_name": gene_claim_name,
                        "concept_id": row.get("concept_id", ""),
                        "nomenclature": row.get("nomenclature", ""),
                        "data_source": "DGIdb"
                    }

    def _parse_drugs(self):
        """Parse drugs.tsv file"""
        self.logger.info("Parsing drugs...")
        
        if not os.path.exists(self.drugs_file):
            self.logger.warning(f"Drugs file not found: {self.drugs_file}")
            return
        
        with open(self.drugs_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                # Use drug_name as primary, fallback to drug_claim_name
                drug_name = row.get("drug_name", "").strip()
                drug_claim_name = row.get("drug_claim_name", "").strip()
                
                if not drug_name or drug_name == 'NULL':
                    drug_name = drug_claim_name
                
                if drug_name and drug_name != 'NULL':
                    # Clean drug name for ID
                    drug_id = drug_name.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
                    
                    self.drugs[drug_id] = {
                        "id": drug_id,
                        "drug_name": drug_name,
                        "drug_claim_name": drug_claim_name,
                        "concept_id": row.get("concept_id", ""),
                        "nomenclature": row.get("nomenclature", ""),
                        "approved": row.get("approved", "FALSE").upper() == "TRUE",
                        "immunotherapy": row.get("immunotherapy", "FALSE").upper() == "TRUE",
                        "anti_neoplastic": row.get("anti_neoplastic", "FALSE").upper() == "TRUE",
                        "data_source": "DGIdb"
                    }

    def _parse_categories(self):
        """Parse categories.tsv file"""
        self.logger.info("Parsing categories...")
        
        if not os.path.exists(self.categories_file):
            self.logger.warning(f"Categories file not found: {self.categories_file}")
            return
        
        with open(self.categories_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                category_name = row.get("name", "").strip()
                
                if category_name:
                    category_id = f"dgidb_category_{category_name.replace(' ', '_')}"
                    
                    self.categories[category_id] = {
                        "id": category_id,
                        "name": category_name,
                        "category_type": row.get("name-2", ""),
                        "data_source": "DGIdb"
                    }

    def _parse_interactions(self):
        """Parse interactions.tsv file"""
        self.logger.info("Parsing interactions...")
        
        if not os.path.exists(self.interactions_file):
            self.logger.warning(f"Interactions file not found: {self.interactions_file}")
            return
        
        with open(self.interactions_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                # Get gene and drug names
                gene_name = row.get("gene_name", "").strip()
                gene_claim_name = row.get("gene_claim_name", "").strip()
                drug_name = row.get("drug_name", "").strip()
                drug_claim_name = row.get("drug_claim_name", "").strip()
                
                # Use primary names, fallback to claim names
                if not gene_name or gene_name == 'NULL':
                    gene_name = gene_claim_name
                if not drug_name or drug_name == 'NULL':
                    drug_name = drug_claim_name
                
                if gene_name and drug_name and gene_name != 'NULL' and drug_name != 'NULL':
                    # Clean names for IDs
                    gene_id = gene_name.replace(' ', '_').replace('-', '_')
                    drug_id = drug_name.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
                    
                    # Add to genes/drugs if not already present (from interactions.tsv)
                    if gene_id not in self.genes:
                        self.genes[gene_id] = {
                            "id": gene_id,
                            "gene_name": gene_name,
                            "gene_claim_name": gene_claim_name,
                            "concept_id": row.get("gene_concept_id", ""),
                            "nomenclature": "",
                            "data_source": "DGIdb"
                        }
                    
                    if drug_id not in self.drugs:
                        self.drugs[drug_id] = {
                            "id": drug_id,
                            "drug_name": drug_name,
                            "drug_claim_name": drug_claim_name,
                            "concept_id": row.get("drug_concept_id", ""),
                            "nomenclature": "",
                            "approved": row.get("approved", "FALSE").upper() == "TRUE",
                            "immunotherapy": row.get("immunotherapy", "FALSE").upper() == "TRUE",
                            "anti_neoplastic": row.get("anti_neoplastic", "FALSE").upper() == "TRUE",
                            "data_source": "DGIdb"
                        }
                    
                    # Store interaction
                    interaction = {
                        "gene_id": gene_id,
                        "drug_id": drug_id,
                        "interaction_type": row.get("interaction_type", ""),
                        "interaction_score": row.get("interaction_score", "0"),
                        "data_source": "DGIdb"
                    }
                    self.interactions.append(interaction)

    def get_nodes(self):
        """Get all nodes for the knowledge graph"""
        # Gene nodes
        for gene_id, gene_data in self.genes.items():
            yield (gene_id, "gene", gene_data)
        
        # Drug nodes
        for drug_id, drug_data in self.drugs.items():
            yield (drug_id, "drug", drug_data)
        
        # Category nodes
        for category_id, category_data in self.categories.items():
            yield (category_id, "gene_category", category_data)

    def get_edges(self):
        """Get all edges for the knowledge graph"""
        
        edge_counter = 0
        
        # Drug-Gene interaction edges
        for interaction in self.interactions:
            edge_counter += 1
            
            # Convert interaction_score to float if possible
            try:
                score = float(interaction['interaction_score']) if interaction['interaction_score'] else 0.0
            except (ValueError, TypeError):
                score = 0.0
            
            properties = {
                'interaction_type': interaction['interaction_type'],
                'interaction_score': score,
                'data_source': interaction['data_source']
            }
            
            yield (
                f"dgidb_interaction_{edge_counter}",  # edge_id
                interaction['drug_id'],               # source: drug
                interaction['gene_id'],               # target: gene
                "DRUG_GENE_INTERACTION",
                properties
            )
        
        # Gene-Category edges (link genes to categories by name matching)
        for gene_id, gene_data in self.genes.items():
            gene_name = gene_data['gene_name']
            
            # Find matching categories
            for category_id, category_data in self.categories.items():
                category_name = category_data['name']
                
                # Simple name matching (can be enhanced)
                if gene_name.upper() == category_name.upper():
                    edge_counter += 1
                    
                    properties = {
                        'data_source': 'DGIdb'
                    }
                    
                    yield (
                        f"dgidb_category_{edge_counter}",  # edge_id
                        gene_id,                           # source: gene
                        category_id,                       # target: category
                        "BELONGS_TO_CATEGORY",
                        properties
                    )

    def _log_statistics(self):
        """Log parsing statistics"""
        self.logger.info(f"Parsed {len(self.genes)} genes")
        self.logger.info(f"Parsed {len(self.drugs)} drugs")
        self.logger.info(f"Parsed {len(self.categories)} categories")
        self.logger.info(f"Parsed {len(self.interactions)} interactions")
