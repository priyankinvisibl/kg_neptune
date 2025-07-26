"""
Adapter for CIViC data
"""

from . import CivicBaseAdapter
import os
import json
import logging
import requests
from datetime import datetime

class CivicAdapter(CivicBaseAdapter):
    """Adapter for CIViC data"""
    
    def __init__(self, data_dir=None):
        """Initialize the adapter"""
        super().__init__(data_dir)
        self.logger = logging.getLogger(__name__)
        
        # File paths
        self.genes_file = os.path.join(self.data_dir, "genes.json")
        self.variants_file = os.path.join(self.data_dir, "variants.json")
        self.evidence_items_file = os.path.join(self.data_dir, "evidence_items.json")
        self.assertions_file = os.path.join(self.data_dir, "assertions.json")
        
        # Data structures
        self.genes = {}
        self.variants = {}
        self.evidence_items = {}
        self.assertions = {}
        self.diseases = {}
        self.drugs = {}
        self.therapies = {}
    
    def download_data(self, force=False):
        """Download data from CIViC API"""
        self.logger.info("Downloading data from CIViC API")
        
        # Download genes
        if force or not os.path.exists(self.genes_file):
            self.logger.info("Downloading genes")
            response = requests.get("https://civicdb.org/api/genes?count=999999")
            response.raise_for_status()
            
            with open(self.genes_file, "w") as f:
                json.dump(response.json(), f)
        
        # Download variants
        if force or not os.path.exists(self.variants_file):
            self.logger.info("Downloading variants")
            response = requests.get("https://civicdb.org/api/variants?count=999999")
            response.raise_for_status()
            
            with open(self.variants_file, "w") as f:
                json.dump(response.json(), f)
        
        # Download evidence items
        if force or not os.path.exists(self.evidence_items_file):
            self.logger.info("Downloading evidence items")
            response = requests.get("https://civicdb.org/api/evidence_items?count=999999")
            response.raise_for_status()
            
            with open(self.evidence_items_file, "w") as f:
                json.dump(response.json(), f)
        
        # Download assertions
        if force or not os.path.exists(self.assertions_file):
            self.logger.info("Downloading assertions")
            response = requests.get("https://civicdb.org/api/assertions?count=999999")
            response.raise_for_status()
            
            with open(self.assertions_file, "w") as f:
                json.dump(response.json(), f)
        
        self.logger.info("Download complete")
    
    def parse_data(self):
        """Parse data from files"""
        self.logger.info("Parsing CIViC data")
        
        # Check if files exist
        if not os.path.exists(self.genes_file):
            self.logger.warning(f"Genes file not found: {self.genes_file}")
            self.download_data()
        
        if not os.path.exists(self.variants_file):
            self.logger.warning(f"Variants file not found: {self.variants_file}")
            self.download_data()
        
        if not os.path.exists(self.evidence_items_file):
            self.logger.warning(f"Evidence items file not found: {self.evidence_items_file}")
            self.download_data()
        
        if not os.path.exists(self.assertions_file):
            self.logger.warning(f"Assertions file not found: {self.assertions_file}")
            self.download_data()
        
        # Parse genes
        self.logger.info("Parsing genes")
        with open(self.genes_file, "r") as f:
            genes_data = json.load(f)
            
            for gene in genes_data["records"]:
                gene_id = f"civic_gene:{gene['id']}"
                self.genes[gene_id] = {
                    "name": gene.get("name", ""),
                    "entrez_id": gene.get("entrez_id"),
                    "description": gene.get("description", ""),
                    "aliases": "|".join(gene.get("aliases", [])),
                    "source": "CIViC",
                    "data_source": "CIViC"
                }
        
        # Parse variants
        self.logger.info("Parsing variants")
        with open(self.variants_file, "r") as f:
            variants_data = json.load(f)
            
            for variant in variants_data["records"]:
                variant_id = f"civic_variant:{variant['id']}"
                gene_id = f"civic_gene:{variant.get('gene_id')}"
                
                self.variants[variant_id] = {
                    "name": variant.get("name", ""),
                    "gene_id": gene_id,
                    "description": variant.get("description", ""),
                    "variant_types": "|".join([vt.get("display_name", "") for vt in variant.get("variant_types", [])]),
                    "source": "CIViC",
                    "data_source": "CIViC"
                }
        
        # Parse evidence items
        self.logger.info("Parsing evidence items")
        with open(self.evidence_items_file, "r") as f:
            evidence_items_data = json.load(f)
            
            for evidence in evidence_items_data["records"]:
                evidence_id = f"civic_evidence:{evidence['id']}"
                variant_id = f"civic_variant:{evidence.get('variant_id')}"
                
                # Extract disease
                if evidence.get("disease"):
                    disease_id = f"civic_disease:{evidence['disease']['id']}"
                    self.diseases[disease_id] = {
                        "name": evidence["disease"].get("name", ""),
                        "doid": evidence["disease"].get("doid", ""),
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                else:
                    disease_id = None
                
                # Extract drugs
                drugs = []
                if evidence.get("drugs"):
                    for drug in evidence["drugs"]:
                        drug_id = f"civic_drug:{drug['id']}"
                        self.drugs[drug_id] = {
                            "name": drug.get("name", ""),
                            "source": "CIViC",
                            "data_source": "CIViC"
                        }
                        drugs.append(drug_id)
                
                # Create evidence item
                self.evidence_items[evidence_id] = {
                    "name": f"Evidence {evidence['id']}",
                    "variant_id": variant_id,
                    "disease_id": disease_id,
                    "drug_ids": "|".join(drugs),
                    "evidence_type": evidence.get("evidence_type", ""),
                    "evidence_level": evidence.get("evidence_level", ""),
                    "clinical_significance": evidence.get("clinical_significance", ""),
                    "evidence_direction": evidence.get("evidence_direction", ""),
                    "source": "CIViC",
                    "data_source": "CIViC"
                }
        
        # Parse assertions
        self.logger.info("Parsing assertions")
        with open(self.assertions_file, "r") as f:
            assertions_data = json.load(f)
            
            for assertion in assertions_data["records"]:
                assertion_id = f"civic_assertion:{assertion['id']}"
                
                # Extract gene and variant
                gene_id = f"civic_gene:{assertion.get('gene_id')}"
                variant_id = f"civic_variant:{assertion.get('variant_id')}"
                
                # Extract disease
                if assertion.get("disease"):
                    disease_id = f"civic_disease:{assertion['disease']['id']}"
                    disease_name = assertion["disease"].get("name", "")
                    doid = assertion["disease"].get("doid", "")
                    
                    self.diseases[disease_id] = {
                        "name": disease_name,
                        "doid": doid,
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                else:
                    disease_id = None
                    disease_name = ""
                    doid = ""
                
                # Extract therapies
                therapies = []
                therapy_names = []
                if assertion.get("therapies"):
                    for therapy in assertion["therapies"]:
                        therapy_id = f"civic_therapy:{therapy['id']}"
                        self.therapies[therapy_id] = {
                            "name": therapy.get("name", ""),
                            "source": "CIViC",
                            "data_source": "CIViC"
                        }
                        therapies.append(therapy_id)
                        therapy_names.append(therapy.get("name", ""))
                
                # Create assertion
                self.assertions[assertion_id] = {
                    "name": f"Assertion {assertion['id']}",
                    "gene_id": gene_id,
                    "variant_id": variant_id,
                    "disease_id": disease_id,
                    "disease": disease_name,
                    "doid": doid,
                    "therapy_ids": "|".join(therapies),
                    "therapies": "|".join(therapy_names),
                    "assertion_type": assertion.get("assertion_type", ""),
                    "assertion_direction": assertion.get("assertion_direction", ""),
                    "clinical_significance": assertion.get("clinical_significance", ""),
                    "amp_category": assertion.get("amp_level", ""),
                    "description": assertion.get("description", ""),
                    "summary": assertion.get("summary", ""),
                    "source": "CIViC",
                    "data_source": "CIViC"
                }
        
        self.logger.info("Parsing complete")
        self.logger.info(f"Parsed {len(self.genes)} genes")
        self.logger.info(f"Parsed {len(self.variants)} variants")
        self.logger.info(f"Parsed {len(self.evidence_items)} evidence items")
        self.logger.info(f"Parsed {len(self.assertions)} assertions")
        self.logger.info(f"Parsed {len(self.diseases)} diseases")
        self.logger.info(f"Parsed {len(self.drugs)} drugs")
        self.logger.info(f"Parsed {len(self.therapies)} therapies")
    
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
                    "entrez_id": gene.get("entrez_id"),
                    "description": gene.get("description", ""),
                    "aliases": gene.get("aliases", ""),
                    "source": gene.get("source", "CIViC"),
                    "data_source": gene.get("data_source", "CIViC")
                }
            )
        
        # Generate variant nodes
        for variant_id, variant in self.variants.items():
            yield (
                variant_id,
                "variant",
                {
                    "name": variant.get("name", ""),
                    "id": variant_id,
                    "description": variant.get("description", ""),
                    "variant_types": variant.get("variant_types", ""),
                    "source": variant.get("source", "CIViC"),
                    "data_source": variant.get("data_source", "CIViC")
                }
            )
        
        # Generate evidence item nodes
        for evidence_id, evidence in self.evidence_items.items():
            yield (
                evidence_id,
                "evidence",
                {
                    "name": evidence.get("name", ""),
                    "id": evidence_id,
                    "evidence_type": evidence.get("evidence_type", ""),
                    "evidence_level": evidence.get("evidence_level", ""),
                    "clinical_significance": evidence.get("clinical_significance", ""),
                    "evidence_direction": evidence.get("evidence_direction", ""),
                    "source": evidence.get("source", "CIViC"),
                    "data_source": evidence.get("data_source", "CIViC")
                }
            )
        
        # Generate assertion nodes
        for assertion_id, assertion in self.assertions.items():
            yield (
                assertion_id,
                "assertion",
                {
                    "name": assertion.get("name", ""),
                    "id": assertion_id,
                    "assertion_type": assertion.get("assertion_type", ""),
                    "assertion_direction": assertion.get("assertion_direction", ""),
                    "clinical_significance": assertion.get("clinical_significance", ""),
                    "amp_category": assertion.get("amp_category", ""),
                    "description": assertion.get("description", ""),
                    "summary": assertion.get("summary", ""),
                    "disease": assertion.get("disease", ""),
                    "doid": assertion.get("doid", ""),
                    "therapies": assertion.get("therapies", ""),
                    "source": assertion.get("source", "CIViC"),
                    "data_source": assertion.get("data_source", "CIViC")
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
                    "doid": disease.get("doid", ""),
                    "source": disease.get("source", "CIViC"),
                    "data_source": disease.get("data_source", "CIViC")
                }
            )
        
        # Generate drug nodes
        for drug_id, drug in self.drugs.items():
            yield (
                drug_id,
                "drug",
                {
                    "name": drug.get("name", ""),
                    "id": drug_id,
                    "source": drug.get("source", "CIViC"),
                    "data_source": drug.get("data_source", "CIViC")
                }
            )
        
        # Generate therapy nodes
        for therapy_id, therapy in self.therapies.items():
            yield (
                therapy_id,
                "therapy",
                {
                    "name": therapy.get("name", ""),
                    "id": therapy_id,
                    "source": therapy.get("source", "CIViC"),
                    "data_source": therapy.get("data_source", "CIViC")
                }
            )
    
    def get_edges(self):
        """Get edges for the knowledge graph"""
        # Generate gene to variant edges
        for variant_id, variant in self.variants.items():
            gene_id = variant.get("gene_id")
            if gene_id:
                yield (
                    gene_id,
                    variant_id,
                    "gene_to_variant",
                    {
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                )
        
        # Generate variant to evidence edges
        for evidence_id, evidence in self.evidence_items.items():
            variant_id = evidence.get("variant_id")
            if variant_id:
                yield (
                    variant_id,
                    evidence_id,
                    "variant_to_evidence",
                    {
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                )
        
        # Generate evidence to disease edges
        for evidence_id, evidence in self.evidence_items.items():
            disease_id = evidence.get("disease_id")
            if disease_id:
                yield (
                    evidence_id,
                    disease_id,
                    "evidence_to_disease",
                    {
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                )
        
        # Generate evidence to drug edges
        for evidence_id, evidence in self.evidence_items.items():
            drug_ids = evidence.get("drug_ids", "").split("|")
            for drug_id in drug_ids:
                if drug_id:
                    yield (
                        evidence_id,
                        drug_id,
                        "evidence_to_drug",
                        {
                            "source": "CIViC",
                            "data_source": "CIViC"
                        }
                    )
        
        # Generate assertion to gene edges
        for assertion_id, assertion in self.assertions.items():
            gene_id = assertion.get("gene_id")
            if gene_id:
                yield (
                    assertion_id,
                    gene_id,
                    "assertion_to_gene",
                    {
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                )
        
        # Generate assertion to variant edges
        for assertion_id, assertion in self.assertions.items():
            variant_id = assertion.get("variant_id")
            if variant_id:
                yield (
                    assertion_id,
                    variant_id,
                    "assertion_to_variant",
                    {
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                )
        
        # Generate assertion to disease edges
        for assertion_id, assertion in self.assertions.items():
            disease_id = assertion.get("disease_id")
            if disease_id:
                yield (
                    assertion_id,
                    disease_id,
                    "assertion_to_disease",
                    {
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                )
        
        # Generate assertion to therapy edges
        for assertion_id, assertion in self.assertions.items():
            therapy_ids = assertion.get("therapy_ids", "").split("|")
            for therapy_id in therapy_ids:
                if therapy_id:
                    yield (
                        assertion_id,
                        therapy_id,
                        "assertion_to_therapy",
                        {
                            "source": "CIViC",
                            "data_source": "CIViC"
                        }
                    )
