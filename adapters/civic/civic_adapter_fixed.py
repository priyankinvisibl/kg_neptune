"""
Fixed CIViC Adapter with correct node and edge relationships
Based on actual CIViC data structure analysis
"""

from . import CivicBaseAdapter
import os
import csv
import logging
import requests
from datetime import datetime

class CivicAdapterFixed(CivicBaseAdapter):
    """Fixed adapter for CIViC data with correct relationships"""
    
    def __init__(self, data_dir=None):
        """Initialize the adapter"""
        super().__init__(data_dir)
        self.logger = logging.getLogger(__name__)
        
        # File paths for TSV files (downloaded from URLs)
        self.features_file = os.path.join(self.data_dir, "01-Jul-2025-FeatureSummaries.tsv")
        self.variants_file = os.path.join(self.data_dir, "01-Jul-2025-VariantSummaries.tsv")
        self.molecular_profiles_file = os.path.join(self.data_dir, "01-Jul-2025-MolecularProfileSummaries.tsv")
        self.evidence_file = os.path.join(self.data_dir, "01-Jul-2025-ClinicalEvidenceSummaries.tsv")
        self.assertions_file = os.path.join(self.data_dir, "01-Jul-2025-AssertionSummaries.tsv")
        
        # Data structures for nodes
        self.features = {}  # Gene/Feature nodes
        self.variants = {}  # Variant nodes
        self.molecular_profiles = {}  # Molecular Profile nodes
        self.evidence_items = {}  # Evidence nodes
        self.assertions = {}  # Assertion nodes
        
        # Additional entities extracted from evidence/assertions
        self.diseases = {}
        self.therapies = {}

    def download_data(self, force=False):
        """Download data from CIViC URLs if files don't exist"""
        self.logger.info("Checking CIViC data files...")
        
        # URLs for CIViC data files
        urls = {
            self.features_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-FeatureSummaries.tsv",
            self.variants_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-VariantSummaries.tsv",
            self.molecular_profiles_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-MolecularProfileSummaries.tsv",
            self.evidence_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-ClinicalEvidenceSummaries.tsv",
            self.assertions_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-AssertionSummaries.tsv"
        }
        
        for file_path, url in urls.items():
            if force or not os.path.exists(file_path):
                self.logger.info(f"Downloading {os.path.basename(file_path)}...")
                try:
                    response = requests.get(url, timeout=300)
                    response.raise_for_status()
                    
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    
                    with open(file_path, "w", encoding='utf-8') as f:
                        f.write(response.text)
                    
                    self.logger.info(f"✅ Downloaded {os.path.basename(file_path)}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Failed to download {os.path.basename(file_path)}: {e}")
                    raise
            else:
                self.logger.info(f"✅ {os.path.basename(file_path)} already exists")

    def parse_data(self):
        """Parse data from CIViC TSV files"""
        self.logger.info("Parsing CIViC data from TSV files")
        
        # Ensure files exist
        self._ensure_files_exist()
        
        # Parse in order: Features → Variants → Molecular Profiles → Evidence → Assertions
        self._parse_features()
        self._parse_variants()
        self._parse_molecular_profiles()
        self._parse_evidence()
        self._parse_assertions()
        
        self.logger.info("CIViC data parsing complete")
        self._log_statistics()

    def _ensure_files_exist(self):
        """Ensure all required files exist, download if missing"""
        missing_files = []
        for file_path in [self.features_file, self.variants_file, self.molecular_profiles_file, 
                         self.evidence_file, self.assertions_file]:
            if not os.path.exists(file_path):
                missing_files.append(file_path)
        
        if missing_files:
            self.logger.info("Some files missing, downloading...")
            self.download_data()

    def _parse_features(self):
        """Parse Feature Summary File (Gene nodes)"""
        self.logger.info("Parsing features (genes)...")
        
        with open(self.features_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                feature_id = f"civic_feature:{row['feature_id']}"
                
                self.features[feature_id] = {
                    "id": feature_id,
                    "name": row.get("name", ""),
                    "feature_type": row.get("feature_type", ""),
                    "description": row.get("description", ""),
                    "aliases": row.get("feature_aliases", ""),
                    "entrez_id": row.get("entrez_id", ""),
                    "civic_url": row.get("feature_civic_url", ""),
                    "data_source": "CIViC"
                }

    def _parse_variants(self):
        """Parse Variant Summary File (Variant nodes)"""
        self.logger.info("Parsing variants...")
        
        with open(self.variants_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                variant_id = f"civic_variant:{row['variant_id']}"
                
                self.variants[variant_id] = {
                    "id": variant_id,
                    "name": row.get("variant", ""),
                    "feature_id": f"civic_feature:{row['feature_id']}" if row.get('feature_id') else None,
                    "feature_name": row.get("feature_name", ""),
                    "variant_types": row.get("variant_types", ""),
                    "aliases": row.get("variant_aliases", ""),
                    "chromosome": row.get("chromosome", ""),
                    "start": row.get("start", ""),
                    "stop": row.get("stop", ""),
                    "reference_bases": row.get("reference_bases", ""),
                    "variant_bases": row.get("variant_bases", ""),
                    "civic_url": row.get("variant_civic_url", ""),
                    "data_source": "CIViC"
                }

    def _parse_molecular_profiles(self):
        """Parse Molecular Profile Summary File (Molecular Profile nodes)"""
        self.logger.info("Parsing molecular profiles...")
        
        with open(self.molecular_profiles_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                profile_id = f"civic_molecular_profile:{row['molecular_profile_id']}"
                
                self.molecular_profiles[profile_id] = {
                    "id": profile_id,
                    "name": row.get("name", ""),
                    "summary": row.get("summary", ""),
                    "variant_ids": row.get("variant_ids", ""),  # For MP → Variant edges
                    "evidence_item_ids": row.get("evidence_item_ids", ""),  # For MP → Evidence edges
                    "assertion_ids": row.get("assertion_ids", ""),
                    "evidence_score": row.get("evidence_score", ""),
                    "aliases": row.get("aliases", ""),
                    "data_source": "CIViC"
                }

    def _parse_evidence(self):
        """Parse Clinical Evidence Summary File (Evidence nodes)"""
        self.logger.info("Parsing evidence items...")
        
        with open(self.evidence_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                evidence_id = f"civic_evidence:{row['evidence_id']}"
                
                # Extract disease and therapy info
                self._extract_disease_from_evidence(row)
                self._extract_therapies_from_evidence(row)
                
                self.evidence_items[evidence_id] = {
                    "id": evidence_id,
                    "molecular_profile_id": f"civic_molecular_profile:{row['molecular_profile_id']}" if row.get('molecular_profile_id') else None,
                    "molecular_profile": row.get("molecular_profile", ""),
                    "disease": row.get("disease", ""),
                    "doid": row.get("doid", ""),
                    "therapies": row.get("therapies", ""),
                    "evidence_type": row.get("evidence_type", ""),
                    "evidence_direction": row.get("evidence_direction", ""),
                    "evidence_level": row.get("evidence_level", ""),
                    "significance": row.get("significance", ""),
                    "evidence_statement": row.get("evidence_statement", ""),
                    "citation": row.get("citation", ""),
                    "rating": row.get("rating", ""),
                    "evidence_status": row.get("evidence_status", ""),
                    "civic_url": row.get("evidence_civic_url", ""),
                    "data_source": "CIViC"
                }

    def _parse_assertions(self):
        """Parse Assertion Summary File (Assertion nodes)"""
        self.logger.info("Parsing assertions...")
        
        with open(self.assertions_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                assertion_id = f"civic_assertion:{row['assertion_id']}"
                
                # Extract disease and therapy info
                self._extract_disease_from_assertion(row)
                self._extract_therapies_from_assertion(row)
                
                self.assertions[assertion_id] = {
                    "id": assertion_id,
                    "molecular_profile_id": f"civic_molecular_profile:{row['molecular_profile_id']}" if row.get('molecular_profile_id') else None,
                    "molecular_profile": row.get("molecular_profile", ""),
                    "disease": row.get("disease", ""),
                    "doid": row.get("doid", ""),
                    "therapies": row.get("therapies", ""),
                    "assertion_type": row.get("assertion_type", ""),
                    "assertion_direction": row.get("assertion_direction", ""),
                    "significance": row.get("significance", ""),
                    "amp_category": row.get("amp_category", ""),
                    "nccn_guideline": row.get("nccn_guideline", ""),
                    "regulatory_approval": row.get("regulatory_approval", ""),
                    "assertion_summary": row.get("assertion_summary", ""),
                    "assertion_description": row.get("assertion_description", ""),
                    "evidence_item_ids": row.get("evidence_item_ids", ""),  # For Evidence → Assertion edges
                    "civic_url": row.get("assertion_civic_url", ""),
                    "data_source": "CIViC"
                }

    def _extract_disease_from_evidence(self, row):
        """Extract disease information from evidence row"""
        if row.get("disease") and row.get("doid"):
            disease_id = f"civic_disease:{row['doid']}"
            self.diseases[disease_id] = {
                "id": disease_id,
                "name": row["disease"],
                "doid": row["doid"],
                "data_source": "CIViC"
            }

    def _extract_disease_from_assertion(self, row):
        """Extract disease information from assertion row"""
        if row.get("disease") and row.get("doid"):
            disease_id = f"civic_disease:{row['doid']}"
            self.diseases[disease_id] = {
                "id": disease_id,
                "name": row["disease"],
                "doid": row["doid"],
                "data_source": "CIViC"
            }

    def _extract_therapies_from_evidence(self, row):
        """Extract therapy information from evidence row"""
        if row.get("therapies"):
            therapies = [t.strip() for t in row["therapies"].split(",") if t.strip()]
            for therapy_name in therapies:
                therapy_id = f"civic_therapy:{therapy_name.replace(' ', '_').replace('/', '_')}"
                self.therapies[therapy_id] = {
                    "id": therapy_id,
                    "name": therapy_name,
                    "data_source": "CIViC"
                }

    def _extract_therapies_from_assertion(self, row):
        """Extract therapy information from assertion row"""
        if row.get("therapies"):
            therapies = [t.strip() for t in row["therapies"].split(",") if t.strip()]
            for therapy_name in therapies:
                therapy_id = f"civic_therapy:{therapy_name.replace(' ', '_').replace('/', '_')}"
                self.therapies[therapy_id] = {
                    "id": therapy_id,
                    "name": therapy_name,
                    "data_source": "CIViC"
                }

    def get_nodes(self):
        """Get all nodes for the knowledge graph"""
        # Feature nodes (not Gene - these are Features in CIViC)
        feature_count = 0
        for feature_id, feature in self.features.items():
            feature_count += 1
            yield (feature_id, "feature", feature)
        
        self.logger.info(f"Generated {feature_count} feature nodes")
        
        # Variant nodes
        variant_count = 0
        for variant_id, variant in self.variants.items():
            variant_count += 1
            yield (variant_id, "variant", variant)
        
        self.logger.info(f"Generated {variant_count} variant nodes")
        
        # Molecular Profile nodes
        profile_count = 0
        for profile_id, profile in self.molecular_profiles.items():
            profile_count += 1
            yield (profile_id, "molecular_profile", profile)
        
        self.logger.info(f"Generated {profile_count} molecular_profile nodes")
        
        # Evidence nodes
        evidence_count = 0
        for evidence_id, evidence in self.evidence_items.items():
            evidence_count += 1
            yield (evidence_id, "evidence", evidence)
        
        self.logger.info(f"Generated {evidence_count} evidence nodes")
        
        # Assertion nodes
        assertion_count = 0
        for assertion_id, assertion in self.assertions.items():
            assertion_count += 1
            yield (assertion_id, "assertion", assertion)
        
        self.logger.info(f"Generated {assertion_count} assertion nodes")
        
        # Disease nodes
        disease_count = 0
        for disease_id, disease in self.diseases.items():
            disease_count += 1
            yield (disease_id, "disease", disease)
        
        self.logger.info(f"Generated {disease_count} disease nodes")
        
        # Therapy nodes
        therapy_count = 0
        for therapy_id, therapy in self.therapies.items():
            therapy_count += 1
            yield (therapy_id, "therapy", therapy)
        
        self.logger.info(f"Generated {therapy_count} therapy nodes")

    def get_edges(self):
        """Get all edges for the knowledge graph with proper IDs"""
        
        edge_counter = 0
        
        # 1. Feature → Variant edges
        for variant_id, variant in self.variants.items():
            if variant.get("feature_id"):
                edge_counter += 1
                yield (
                    f"civic_edge_{edge_counter}",  # edge_id
                    variant["feature_id"],         # source: feature
                    variant_id,                    # target: variant
                    "HAS_VARIANT",
                    {"data_source": "CIViC"}
                )
        
        # 2. Molecular Profile → Variant edges (one MP can have multiple variants)
        for profile_id, profile in self.molecular_profiles.items():
            variant_ids_str = profile.get("variant_ids", "")
            if variant_ids_str:
                variant_ids = [vid.strip() for vid in variant_ids_str.split(",") if vid.strip()]
                for variant_id_num in variant_ids:
                    variant_id = f"civic_variant:{variant_id_num}"
                    if variant_id in self.variants:
                        edge_counter += 1
                        yield (
                            f"civic_edge_{edge_counter}",  # edge_id
                            profile_id,                    # source: molecular_profile
                            variant_id,                    # target: variant
                            "INCLUDES_VARIANT",
                            {"data_source": "CIViC"}
                        )
        
        # 3. Molecular Profile → Evidence edges (1:1 association)
        for evidence_id, evidence in self.evidence_items.items():
            if evidence.get("molecular_profile_id"):
                edge_counter += 1
                yield (
                    f"civic_edge_{edge_counter}",      # edge_id
                    evidence["molecular_profile_id"],  # source: molecular_profile
                    evidence_id,                       # target: evidence
                    "HAS_EVIDENCE",
                    {"data_source": "CIViC"}
                )
        
        # 4. Evidence → Assertion edges (one assertion has multiple evidence items)
        for assertion_id, assertion in self.assertions.items():
            evidence_ids_str = assertion.get("evidence_item_ids", "")
            if evidence_ids_str:
                evidence_ids = [eid.strip() for eid in evidence_ids_str.split(",") if eid.strip()]
                for evidence_id_num in evidence_ids:
                    evidence_id = f"civic_evidence:{evidence_id_num}"
                    if evidence_id in self.evidence_items:
                        edge_counter += 1
                        yield (
                            f"civic_edge_{edge_counter}",  # edge_id
                            evidence_id,                   # source: evidence
                            assertion_id,                  # target: assertion
                            "SUPPORTS_ASSERTION",
                            {"data_source": "CIViC"}
                        )
        
        # 5. Evidence → Disease edges
        for evidence_id, evidence in self.evidence_items.items():
            if evidence.get("doid"):
                disease_id = f"civic_disease:{evidence['doid']}"
                if disease_id in self.diseases:
                    edge_counter += 1
                    yield (
                        f"civic_edge_{edge_counter}",  # edge_id
                        evidence_id,
                        disease_id,
                        "ASSOCIATED_WITH_DISEASE",
                        {"data_source": "CIViC"}
                    )
        
        # 6. Evidence → Therapy edges
        for evidence_id, evidence in self.evidence_items.items():
            if evidence.get("therapies"):
                therapies = [t.strip() for t in evidence["therapies"].split(",") if t.strip()]
                for therapy_name in therapies:
                    therapy_id = f"civic_therapy:{therapy_name.replace(' ', '_').replace('/', '_')}"
                    if therapy_id in self.therapies:
                        edge_counter += 1
                        yield (
                            f"civic_edge_{edge_counter}",  # edge_id
                            evidence_id,
                            therapy_id,
                            "EVIDENCE_FOR_THERAPY",
                            {"data_source": "CIViC"}
                        )
        
        # 7. Assertion → Disease edges
        for assertion_id, assertion in self.assertions.items():
            if assertion.get("doid"):
                disease_id = f"civic_disease:{assertion['doid']}"
                if disease_id in self.diseases:
                    edge_counter += 1
                    yield (
                        f"civic_edge_{edge_counter}",  # edge_id
                        assertion_id,
                        disease_id,
                        "ASSERTION_FOR_DISEASE",
                        {"data_source": "CIViC"}
                    )
        
        # 8. Assertion → Therapy edges
        for assertion_id, assertion in self.assertions.items():
            if assertion.get("therapies"):
                therapies = [t.strip() for t in assertion["therapies"].split(",") if t.strip()]
                for therapy_name in therapies:
                    therapy_id = f"civic_therapy:{therapy_name.replace(' ', '_').replace('/', '_')}"
                    if therapy_id in self.therapies:
                        edge_counter += 1
                        yield (
                            f"civic_edge_{edge_counter}",  # edge_id
                            assertion_id,
                            therapy_id,
                            "ASSERTION_FOR_THERAPY",
                            {"data_source": "CIViC"}
                        )

    def _log_statistics(self):
        """Log parsing statistics"""
        self.logger.info(f"Parsed {len(self.features)} features (genes)")
        self.logger.info(f"Parsed {len(self.variants)} variants")
        self.logger.info(f"Parsed {len(self.molecular_profiles)} molecular profiles")
        self.logger.info(f"Parsed {len(self.evidence_items)} evidence items")
        self.logger.info(f"Parsed {len(self.assertions)} assertions")
        self.logger.info(f"Extracted {len(self.diseases)} diseases")
        self.logger.info(f"Extracted {len(self.therapies)} therapies")
