"""
Adapter for CIViC data
"""

from . import CivicBaseAdapter
import os
import csv
import logging
import requests
from datetime import datetime

class CivicAdapter(CivicBaseAdapter):
    """Adapter for CIViC data"""
    
    def __init__(self, data_dir=None):
        """Initialize the adapter"""
        super().__init__(data_dir)
        self.logger = logging.getLogger(__name__)
        
        # File paths for existing TSV files
        self.genes_file = os.path.join(self.data_dir, "01-Jul-2025-FeatureSummaries.tsv")
        self.variants_file = os.path.join(self.data_dir, "01-Jul-2025-VariantSummaries.tsv")
        self.evidence_items_file = os.path.join(self.data_dir, "01-Jul-2025-ClinicalEvidenceSummaries.tsv")
        self.assertions_file = os.path.join(self.data_dir, "01-Jul-2025-AssertionSummaries.tsv")
        self.molecular_profiles_file = os.path.join(self.data_dir, "01-Jul-2025-MolecularProfileSummaries.tsv")
        
        # Data structures
        self.genes = {}
        self.variants = {}
        self.evidence_items = {}
        self.assertions = {}
        self.molecular_profiles = {}
        self.diseases = {}
        self.drugs = {}
        self.therapies = {}
    
    def download_data(self, force=False):
        """Download data from CIViC URLs if files don't exist"""
        self.logger.info("Checking CIViC data files...")
        
        # Default URLs for CIViC data
        urls = {
            self.genes_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-FeatureSummaries.tsv",
            self.variants_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-VariantSummaries.tsv",
            self.molecular_profiles_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-MolecularProfileSummaries.tsv",
            self.evidence_items_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-ClinicalEvidenceSummaries.tsv",
            self.assertions_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-AssertionSummaries.tsv"
        }
        
        for file_path, url in urls.items():
            if force or not os.path.exists(file_path):
                self.logger.info(f"Downloading {os.path.basename(file_path)}...")
                try:
                    response = requests.get(url, timeout=300)  # 5 minute timeout
                    response.raise_for_status()
                    
                    # Ensure directory exists
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    
                    with open(file_path, "w", encoding='utf-8') as f:
                        f.write(response.text)
                    
                    self.logger.info(f"✅ Downloaded {os.path.basename(file_path)}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Failed to download {os.path.basename(file_path)}: {e}")
                    raise
            else:
                self.logger.info(f"✅ {os.path.basename(file_path)} already exists")
        
        self.logger.info("CIViC data files ready")
    
    def parse_data(self):
        """Parse data from existing TSV files"""
        self.logger.info("Parsing CIViC data from existing files")
        
        # Check if files exist
        if not os.path.exists(self.genes_file):
            self.logger.error(f"Genes file not found: {self.genes_file}")
            raise FileNotFoundError(f"Required file not found: {self.genes_file}")
        
        if not os.path.exists(self.variants_file):
            self.logger.error(f"Variants file not found: {self.variants_file}")
            raise FileNotFoundError(f"Required file not found: {self.variants_file}")
        
        if not os.path.exists(self.evidence_items_file):
            self.logger.error(f"Evidence items file not found: {self.evidence_items_file}")
            raise FileNotFoundError(f"Required file not found: {self.evidence_items_file}")
        
        if not os.path.exists(self.assertions_file):
            self.logger.error(f"Assertions file not found: {self.assertions_file}")
            raise FileNotFoundError(f"Required file not found: {self.assertions_file}")
        
        # Parse genes (features)
        self.logger.info("Parsing genes/features")
        with open(self.genes_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                feature_id = f"civic_gene:{row['feature_id']}"
                self.genes[feature_id] = {
                    "name": row.get("name", ""),
                    "entrez_id": row.get("entrez_id", ""),
                    "description": row.get("description", ""),
                    "aliases": row.get("feature_aliases", ""),
                    "feature_type": row.get("feature_type", ""),
                    "source": "CIViC",
                    "data_source": "CIViC"
                }
        
        # Parse molecular profiles
        if os.path.exists(self.molecular_profiles_file):
            self.logger.info("Parsing molecular profiles")
            with open(self.molecular_profiles_file, "r", encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='\t')
                for row in reader:
                    profile_id = f"civic_molecular_profile:{row['molecular_profile_id']}"
                    self.molecular_profiles[profile_id] = {
                        "name": row.get("name", ""),
                        "summary": row.get("summary", ""),
                        "variant_ids": row.get("variant_ids", ""),
                        "evidence_score": row.get("evidence_score", ""),
                        "aliases": row.get("aliases", ""),
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
        
        # Parse variants
        self.logger.info("Parsing variants")
        with open(self.variants_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                variant_id = f"civic_variant:{row['variant_id']}"
                feature_id = f"civic_gene:{row['feature_id']}" if row.get('feature_id') else None
                
                self.variants[variant_id] = {
                    "name": row.get("variant", ""),
                    "feature_id": feature_id,
                    "feature_name": row.get("feature_name", ""),
                    "feature_type": row.get("feature_type", ""),
                    "variant_types": row.get("variant_types", ""),
                    "aliases": row.get("variant_aliases", ""),
                    "gene": row.get("gene", ""),
                    "entrez_id": row.get("entrez_id", ""),
                    "chromosome": row.get("chromosome", ""),
                    "start": row.get("start", ""),
                    "stop": row.get("stop", ""),
                    "reference_bases": row.get("reference_bases", ""),
                    "variant_bases": row.get("variant_bases", ""),
                    "source": "CIViC",
                    "data_source": "CIViC"
                }
        
        # Parse evidence items
        self.logger.info("Parsing evidence items")
        with open(self.evidence_items_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                evidence_id = f"civic_evidence:{row['evidence_id']}"
                molecular_profile_id = f"civic_molecular_profile:{row['molecular_profile_id']}"
                
                # Extract disease
                disease_id = None
                if row.get("disease") and row.get("doid"):
                    disease_id = f"civic_disease:{row['doid']}"
                    self.diseases[disease_id] = {
                        "name": row.get("disease", ""),
                        "doid": row.get("doid", ""),
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                
                # Extract therapies (drugs)
                therapy_ids = []
                if row.get("therapies"):
                    therapies = [t.strip() for t in row["therapies"].split(",") if t.strip()]
                    for therapy_name in therapies:
                        if therapy_name:
                            therapy_id = f"civic_therapy:{therapy_name.replace(' ', '_').replace('/', '_')}"
                            self.therapies[therapy_id] = {
                                "name": therapy_name,
                                "source": "CIViC",
                                "data_source": "CIViC"
                            }
                            therapy_ids.append(therapy_id)
                
                # Create evidence item
                self.evidence_items[evidence_id] = {
                    "name": f"Evidence {row['evidence_id']}",
                    "molecular_profile_id": molecular_profile_id,
                    "molecular_profile": row.get("molecular_profile", ""),
                    "disease_id": disease_id,
                    "therapy_ids": "|".join(therapy_ids),
                    "evidence_type": row.get("evidence_type", ""),
                    "evidence_level": row.get("evidence_level", ""),
                    "significance": row.get("significance", ""),
                    "evidence_direction": row.get("evidence_direction", ""),
                    "evidence_statement": row.get("evidence_statement", ""),
                    "citation": row.get("citation", ""),
                    "rating": row.get("rating", ""),
                    "evidence_status": row.get("evidence_status", ""),
                    "source": "CIViC",
                    "data_source": "CIViC"
                }
        
        # Parse assertions
        self.logger.info("Parsing assertions")
        with open(self.assertions_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                assertion_id = f"civic_assertion:{row['assertion_id']}"
                molecular_profile_id = f"civic_molecular_profile:{row['molecular_profile_id']}"
                
                # Extract disease
                disease_id = None
                if row.get("disease") and row.get("doid"):
                    disease_id = f"civic_disease:{row['doid']}"
                    self.diseases[disease_id] = {
                        "name": row.get("disease", ""),
                        "doid": row.get("doid", ""),
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                
                # Extract therapies
                therapy_ids = []
                if row.get("therapies"):
                    therapies = [t.strip() for t in row["therapies"].split(",") if t.strip()]
                    for therapy_name in therapies:
                        if therapy_name:
                            therapy_id = f"civic_therapy:{therapy_name.replace(' ', '_').replace('/', '_')}"
                            self.therapies[therapy_id] = {
                                "name": therapy_name,
                                "source": "CIViC",
                                "data_source": "CIViC"
                            }
                            therapy_ids.append(therapy_id)
                
                # Create assertion
                self.assertions[assertion_id] = {
                    "name": f"Assertion {row['assertion_id']}",
                    "molecular_profile_id": molecular_profile_id,
                    "molecular_profile": row.get("molecular_profile", ""),
                    "disease_id": disease_id,
                    "disease": row.get("disease", ""),
                    "doid": row.get("doid", ""),
                    "therapy_ids": "|".join(therapy_ids),
                    "therapies": row.get("therapies", ""),
                    "assertion_type": row.get("assertion_type", ""),
                    "assertion_direction": row.get("assertion_direction", ""),
                    "significance": row.get("significance", ""),
                    "amp_category": row.get("amp_category", ""),
                    "nccn_guideline": row.get("nccn_guideline", ""),
                    "regulatory_approval": row.get("regulatory_approval", ""),
                    "fda_companion_test": row.get("fda_companion_test", ""),
                    "description": row.get("assertion_description", ""),
                    "summary": row.get("assertion_summary", ""),
                    "evidence_item_ids": row.get("evidence_item_ids", ""),
                    "source": "CIViC",
                    "data_source": "CIViC"
                }
        
        self.logger.info("Parsing complete")
        self.logger.info(f"Parsed {len(self.genes)} genes/features")
        self.logger.info(f"Parsed {len(self.molecular_profiles)} molecular profiles")
        self.logger.info(f"Parsed {len(self.variants)} variants")
        self.logger.info(f"Parsed {len(self.evidence_items)} evidence items")
        self.logger.info(f"Parsed {len(self.assertions)} assertions")
        self.logger.info(f"Parsed {len(self.diseases)} diseases")
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
                    "feature_type": gene.get("feature_type", ""),
                    "source": gene.get("source", "CIViC"),
                    "data_source": gene.get("data_source", "CIViC")
                }
            )
        
        # Generate molecular profile nodes
        for profile_id, profile in self.molecular_profiles.items():
            yield (
                profile_id,
                "molecular_profile",
                {
                    "name": profile.get("name", ""),
                    "id": profile_id,
                    "summary": profile.get("summary", ""),
                    "variant_ids": profile.get("variant_ids", ""),
                    "evidence_score": profile.get("evidence_score", ""),
                    "aliases": profile.get("aliases", ""),
                    "source": profile.get("source", "CIViC"),
                    "data_source": profile.get("data_source", "CIViC")
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
                    "feature_name": variant.get("feature_name", ""),
                    "feature_type": variant.get("feature_type", ""),
                    "variant_types": variant.get("variant_types", ""),
                    "aliases": variant.get("aliases", ""),
                    "gene": variant.get("gene", ""),
                    "entrez_id": variant.get("entrez_id", ""),
                    "chromosome": variant.get("chromosome", ""),
                    "start": variant.get("start", ""),
                    "stop": variant.get("stop", ""),
                    "reference_bases": variant.get("reference_bases", ""),
                    "variant_bases": variant.get("variant_bases", ""),
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
                    "molecular_profile": evidence.get("molecular_profile", ""),
                    "evidence_type": evidence.get("evidence_type", ""),
                    "evidence_level": evidence.get("evidence_level", ""),
                    "significance": evidence.get("significance", ""),
                    "evidence_direction": evidence.get("evidence_direction", ""),
                    "evidence_statement": evidence.get("evidence_statement", ""),
                    "citation": evidence.get("citation", ""),
                    "rating": evidence.get("rating", ""),
                    "evidence_status": evidence.get("evidence_status", ""),
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
                    "molecular_profile": assertion.get("molecular_profile", ""),
                    "assertion_type": assertion.get("assertion_type", ""),
                    "assertion_direction": assertion.get("assertion_direction", ""),
                    "significance": assertion.get("significance", ""),
                    "amp_category": assertion.get("amp_category", ""),
                    "nccn_guideline": assertion.get("nccn_guideline", ""),
                    "regulatory_approval": assertion.get("regulatory_approval", ""),
                    "fda_companion_test": assertion.get("fda_companion_test", ""),
                    "description": assertion.get("description", ""),
                    "summary": assertion.get("summary", ""),
                    "disease": assertion.get("disease", ""),
                    "doid": assertion.get("doid", ""),
                    "therapies": assertion.get("therapies", ""),
                    "evidence_item_ids": assertion.get("evidence_item_ids", ""),
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
        
        # Generate therapy nodes (replacing drug nodes)
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
        # Generate feature to variant edges (HAS_VARIANT)
        for variant_id, variant in self.variants.items():
            feature_id = variant.get("feature_id")
            if feature_id:
                yield (
                    feature_id,
                    variant_id,
                    "HAS_VARIANT",
                    {
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                )
        
        # Generate molecular profile to variant edges (INCLUDES_VARIANT)
        for profile_id, profile in self.molecular_profiles.items():
            variant_ids_str = profile.get("variant_ids", "")
            if variant_ids_str:
                # Parse variant IDs (can be comma-separated)
                variant_ids = [vid.strip() for vid in variant_ids_str.split(",") if vid.strip()]
                for variant_id_num in variant_ids:
                    variant_id = f"civic_variant:{variant_id_num}"
                    if variant_id in self.variants:
                        yield (
                            profile_id,
                            variant_id,
                            "INCLUDES_VARIANT",
                            {
                                "source": "CIViC",
                                "data_source": "CIViC"
                            }
                        )
        
        # Generate molecular profile to evidence edges (HAS_EVIDENCE)
        for evidence_id, evidence in self.evidence_items.items():
            profile_id = evidence.get("molecular_profile_id")
            if profile_id and profile_id in self.molecular_profiles:
                yield (
                    profile_id,
                    evidence_id,
                    "HAS_EVIDENCE",
                    {
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                )
        
        # Generate molecular profile to assertion edges (HAS_ASSERTION)
        for assertion_id, assertion in self.assertions.items():
            profile_id = assertion.get("molecular_profile_id")
            if profile_id and profile_id in self.molecular_profiles:
                yield (
                    profile_id,
                    assertion_id,
                    "HAS_ASSERTION",
                    {
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                )
        
        # Generate assertion to evidence edges (SUPPORTS_ASSERTION)
        for assertion_id, assertion in self.assertions.items():
            evidence_ids_str = assertion.get("evidence_item_ids", "")
            if evidence_ids_str:
                # Parse evidence IDs (can be comma-separated)
                evidence_ids = [eid.strip() for eid in evidence_ids_str.split(",") if eid.strip()]
                for evidence_id_num in evidence_ids:
                    evidence_id = f"civic_evidence:{evidence_id_num}"
                    if evidence_id in self.evidence_items:
                        yield (
                            evidence_id,
                            assertion_id,
                            "SUPPORTS_ASSERTION",
                            {
                                "source": "CIViC",
                                "data_source": "CIViC"
                            }
                        )
        
        # Generate evidence to disease edges (ASSOCIATED_WITH_DISEASE)
        for evidence_id, evidence in self.evidence_items.items():
            disease_id = evidence.get("disease_id")
            if disease_id and disease_id in self.diseases:
                yield (
                    evidence_id,
                    disease_id,
                    "ASSOCIATED_WITH_DISEASE",
                    {
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                )
        
        # Generate evidence to therapy edges (EVIDENCE_FOR_THERAPY)
        for evidence_id, evidence in self.evidence_items.items():
            therapy_ids = evidence.get("therapy_ids", "").split("|")
            for therapy_id in therapy_ids:
                if therapy_id and therapy_id in self.therapies:
                    yield (
                        evidence_id,
                        therapy_id,
                        "EVIDENCE_FOR_THERAPY",
                        {
                            "source": "CIViC",
                            "data_source": "CIViC"
                        }
                    )
        
        # Generate assertion to disease edges (ASSERTION_FOR_DISEASE)
        for assertion_id, assertion in self.assertions.items():
            disease_id = assertion.get("disease_id")
            if disease_id and disease_id in self.diseases:
                yield (
                    assertion_id,
                    disease_id,
                    "ASSERTION_FOR_DISEASE",
                    {
                        "source": "CIViC",
                        "data_source": "CIViC"
                    }
                )
        
        # Generate assertion to therapy edges (ASSERTION_FOR_THERAPY)
        for assertion_id, assertion in self.assertions.items():
            therapy_ids = assertion.get("therapy_ids", "").split("|")
            for therapy_id in therapy_ids:
                if therapy_id and therapy_id in self.therapies:
                    yield (
                        assertion_id,
                        therapy_id,
                        "ASSERTION_FOR_THERAPY",
                        {
                            "source": "CIViC",
                            "data_source": "CIViC"
                        }
                    )
