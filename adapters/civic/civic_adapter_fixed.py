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
        self.variant_groups_file = os.path.join(self.data_dir, "01-Jul-2025-VariantGroupSummaries.tsv")
        self.molecular_profiles_file = os.path.join(self.data_dir, "01-Jul-2025-MolecularProfileSummaries.tsv")
        self.evidence_file = os.path.join(self.data_dir, "01-Jul-2025-ClinicalEvidenceSummaries.tsv")
        self.assertions_file = os.path.join(self.data_dir, "01-Jul-2025-AssertionSummaries.tsv")
        
        # Data structures for nodes
        self.features = {}  # Gene/Feature nodes
        self.variants = {}  # Variant nodes
        self.variant_groups = {}  # Variant group descriptions
        self.molecular_profiles = {}  # Molecular Profile nodes
        self.evidence_items = {}  # Evidence nodes
        self.assertions = {}  # Assertion nodes
        
        # Additional entities extracted from evidence/assertions
        self.diseases = {}
        self.therapies = {}
        
        # Gene and fusion nodes extracted from features
        self.genes = {}
        self.fusions = {}

    def download_data(self, force=False):
        """Download data from CIViC URLs if files don't exist"""
        self.logger.info("Checking CIViC data files...")
        
        # URLs for CIViC data files
        urls = {
            self.features_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-FeatureSummaries.tsv",
            self.variants_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-VariantSummaries.tsv",
            self.variant_groups_file: "https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-VariantGroupSummaries.tsv",
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
        
        # Parse in order: Features → Variant Groups → Variants → Molecular Profiles → Evidence → Assertions
        self._parse_features()
        self._parse_variant_groups()
        self._parse_variants()
        self._parse_molecular_profiles()
        self._parse_evidence()
        self._parse_assertions()
        
        self.logger.info("CIViC data parsing complete")
        self._log_statistics()

    def _ensure_files_exist(self):
        """Ensure all required files exist, download if missing"""
        missing_files = []
        for file_path in [self.features_file, self.variants_file, self.variant_groups_file, 
                         self.molecular_profiles_file, self.evidence_file, self.assertions_file]:
            if not os.path.exists(file_path):
                missing_files.append(file_path)
        
        if missing_files:
            self.logger.info("Some files missing, downloading...")
            self.download_data()

    def _parse_features(self):
        """Parse Feature Summary File (Feature nodes) with all required columns and extract Gene/Fusion nodes"""
        self.logger.info("Parsing features and extracting genes/fusions...")
        
        with open(self.features_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                feature_id = f"civic_feature:{row['feature_id']}"
                
                # Create Feature node with all required columns
                self.features[feature_id] = {
                    # Required columns
                    "feature_id": row.get("feature_id", ""),
                    "feature_civic_url": row.get("feature_civic_url", ""),
                    "feature_type": row.get("feature_type", ""),
                    "name": row.get("name", ""),
                    "description": row.get("description", ""),
                    
                    # Additional metadata
                    "id": feature_id,
                    "aliases": row.get("feature_aliases", ""),
                    "entrez_id": row.get("entrez_id", ""),
                    "data_source": "CIViC"
                }
                
                # Extract Gene or Fusion node based on feature type
                self._extract_gene_or_fusion_from_feature(row, feature_id)

    def _parse_variant_groups(self):
        """Parse Variant Group Summary File to get descriptions"""
        self.logger.info("Parsing variant groups...")
        
        with open(self.variant_groups_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                variant_group_name = row.get("variant_group", "").strip()
                if variant_group_name:
                    self.variant_groups[variant_group_name] = row.get("description", "")

    def _parse_variants(self):
        """Parse Variant Summary File (Variant nodes) with all 20 required columns"""
        self.logger.info("Parsing variants...")
        
        with open(self.variants_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                variant_id = f"civic_variant:{row['variant_id']}"
                
                # Get variant group description
                variant_groups = row.get("variant_groups", "")
                variant_group_description = ""
                if variant_groups:
                    # Get description for the first variant group
                    group_names = [g.strip() for g in variant_groups.split(",") if g.strip()]
                    if group_names and group_names[0] in self.variant_groups:
                        variant_group_description = self.variant_groups[group_names[0]]
                
                self.variants[variant_id] = {
                    # Required 20 columns
                    "variant_id": row.get("variant_id", ""),
                    "feature_type": row.get("feature_type", ""),
                    "feature_id": row.get("feature_id", ""),
                    "feature_name": row.get("feature_name", ""),
                    "variant": row.get("variant", ""),
                    "variant_aliases": row.get("variant_aliases", ""),
                    "variant_groups": variant_groups,
                    "variant_group_description": variant_group_description,
                    "variant_types": row.get("variant_types", ""),
                    "gene": row.get("gene", ""),
                    "chromosome": row.get("chromosome", ""),
                    "start": row.get("start", ""),
                    "stop": row.get("stop", ""),
                    "reference_bases": row.get("reference_bases", ""),
                    "variant_bases": row.get("variant_bases", ""),
                    "representative_transcript": row.get("representative_transcript", ""),
                    "ensembl_version": row.get("ensembl_version", ""),
                    "reference_build": row.get("reference_build", ""),
                    "hgvs_descriptions": row.get("hgvs_descriptions", ""),
                    "clinvar_ids": row.get("clinvar_ids", ""),
                    
                    # Additional metadata
                    "id": variant_id,
                    "name": row.get("variant", ""),
                    "civic_url": row.get("variant_civic_url", ""),
                    "data_source": "CIViC"
                }

    def _parse_molecular_profiles(self):
        """Parse Molecular Profile Summary File (Molecular Profile nodes) with all required columns"""
        self.logger.info("Parsing molecular profiles...")
        
        with open(self.molecular_profiles_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                profile_id = f"civic_molecular_profile:{row['molecular_profile_id']}"
                
                # Convert variant_ids to full format (civic_variant:ID)
                variant_ids_raw = row.get("variant_ids", "")
                variant_ids_formatted = ""
                if variant_ids_raw:
                    ids = [vid.strip() for vid in variant_ids_raw.split(",") if vid.strip()]
                    formatted_ids = [f"civic_variant:{vid}" for vid in ids]
                    variant_ids_formatted = ",".join(formatted_ids)
                
                # Convert evidence_item_ids to full format (civic_evidence:ID)
                evidence_ids_raw = row.get("evidence_item_ids", "")
                evidence_ids_formatted = ""
                if evidence_ids_raw:
                    ids = [eid.strip() for eid in evidence_ids_raw.split(",") if eid.strip()]
                    formatted_ids = [f"civic_evidence:{eid}" for eid in ids]
                    evidence_ids_formatted = ",".join(formatted_ids)
                
                self.molecular_profiles[profile_id] = {
                    # Required columns
                    "name": row.get("name", ""),
                    "molecular_profile_id": row.get("molecular_profile_id", ""),
                    "summary": row.get("summary", ""),
                    "variant_ids": variant_ids_formatted,  # Formatted to match variant node IDs
                    "variants_civic_url": row.get("variants_civic_url", ""),
                    "evidence_score": row.get("evidence_score", ""),
                    "evidence_item_ids": evidence_ids_formatted,  # Formatted to match evidence node IDs
                    "assertion_ids": row.get("assertion_ids", ""),
                    "aliases": row.get("aliases", ""),
                    
                    # Additional metadata
                    "id": profile_id,
                    "data_source": "CIViC"
                }

    def _parse_evidence(self):
        """Parse Clinical Evidence Summary File (Evidence nodes) with all required columns"""
        self.logger.info("Parsing evidence items...")
        
        with open(self.evidence_file, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                evidence_id = f"civic_evidence:{row['evidence_id']}"
                
                # Extract disease and therapy info
                self._extract_disease_from_evidence(row)
                self._extract_therapies_from_evidence(row)
                
                self.evidence_items[evidence_id] = {
                    # Required columns
                    "molecular_profile": row.get("molecular_profile", ""),
                    "molecular_profile_id": row.get("molecular_profile_id", ""),
                    "disease": row.get("disease", ""),
                    "doid": row.get("doid", ""),
                    "phenotypes": row.get("phenotypes", ""),
                    "therapies": row.get("therapies", ""),
                    "therapy_interaction_type": row.get("therapy_interaction_type", ""),
                    "evidence_type": row.get("evidence_type", ""),
                    "evidence_direction": row.get("evidence_direction", ""),
                    "evidence_level": row.get("evidence_level", ""),
                    "significance": row.get("significance", ""),
                    "evidence_statement": row.get("evidence_statement", ""),
                    "citation_id": row.get("citation_id", ""),
                    "source_type": row.get("source_type", ""),
                    "asco_abstract_id": row.get("asco_abstract_id", ""),
                    "citation": row.get("citation", ""),
                    "nct_ids": row.get("nct_ids", ""),
                    "rating": row.get("rating", ""),
                    "evidence_id": row.get("evidence_id", ""),
                    "variant_origin": row.get("variant_origin", ""),
                    "evidence_civic_url": row.get("evidence_civic_url", ""),
                    "molecular_profile_civic_url": row.get("molecular_profile_civic_url", ""),
                    
                    # Additional metadata
                    "id": evidence_id,
                    "name": row.get("molecular_profile", ""),
                    "evidence_status": row.get("evidence_status", ""),
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
                
                # Convert evidence_item_ids to full format (civic_evidence:ID)
                evidence_ids_raw = row.get("evidence_item_ids", "")
                evidence_ids_formatted = ""
                if evidence_ids_raw:
                    ids = [eid.strip() for eid in evidence_ids_raw.split(",") if eid.strip()]
                    formatted_ids = [f"civic_evidence:{eid}" for eid in ids]
                    evidence_ids_formatted = ",".join(formatted_ids)
                
                self.assertions[assertion_id] = {
                    # Required columns
                    "assertion_direction": row.get("assertion_direction", ""),
                    "significance": row.get("significance", ""),
                    "acmg_codes": row.get("acmg_codes", ""),
                    "amp_category": row.get("amp_category", ""),
                    "nccn_guideline": row.get("nccn_guideline", ""),
                    "regulatory_approval": row.get("regulatory_approval", ""),
                    "fda_companion_test": row.get("fda_companion_test", ""),
                    "assertion_summary": row.get("assertion_summary", ""),
                    "assertion_description": row.get("assertion_description", ""),
                    "assertion_id": row.get("assertion_id", ""),
                    "evidence_item_ids": evidence_ids_formatted,
                    "assertion_civic_url": row.get("assertion_civic_url", ""),
                    
                    # Existing columns
                    "id": assertion_id,
                    "molecular_profile_id": f"civic_molecular_profile:{row['molecular_profile_id']}" if row.get('molecular_profile_id') else None,
                    "molecular_profile": row.get("molecular_profile", ""),
                    "disease": row.get("disease", ""),
                    "doid": row.get("doid", ""),
                    "therapies": row.get("therapies", ""),
                    "assertion_type": row.get("assertion_type", ""),
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

    def _extract_gene_or_fusion_from_feature(self, row, feature_id):
        """Extract Gene or Fusion node from Feature data based on feature_type"""
        feature_type = row.get("feature_type", "").lower()
        name = row.get("name", "").strip()
        entrez_id = row.get("entrez_id", "").strip()
        
        if feature_type == "gene" and name and entrez_id:
            # Create gene node
            gene_id = name
            if gene_id not in self.genes:
                self.genes[gene_id] = {
                    "id": gene_id,
                    "entrez_id": entrez_id,
                    "data_source": "CIViC",
                    "civic_feature_id": feature_id
                }
                self.logger.debug(f"Extracted gene: {gene_id} (Entrez: {entrez_id}) from feature {feature_id}")
        
        elif feature_type == "fusion" and name:
            # Create fusion node
            fusion_id = name
            if fusion_id not in self.fusions:
                self.fusions[fusion_id] = {
                    "id": fusion_id,
                    "data_source": "CIViC",
                    "civic_feature_id": feature_id
                }
                self.logger.debug(f"Extracted fusion: {fusion_id} from feature {feature_id}")

    def _extract_therapies_from_evidence(self, row):
        """Extract therapy information from evidence row"""
        if row.get("therapies"):
            therapies = [t.strip() for t in row["therapies"].split(",") if t.strip()]
            for therapy_name in therapies:
                # Use therapy name directly as ID (cleaned)
                therapy_id = therapy_name.strip()
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
                # Use therapy name directly as ID (cleaned)
                therapy_id = therapy_name.strip()
                self.therapies[therapy_id] = {
                    "id": therapy_id,
                    "name": therapy_name,
                    "data_source": "CIViC"
                }

    def get_nodes(self):
        """Get all nodes for the knowledge graph"""
        # Gene nodes (extracted from features)
        gene_count = 0
        for gene_id, gene in self.genes.items():
            gene_count += 1
            yield (gene_id, "gene", gene)
        
        self.logger.info(f"Generated {gene_count} gene nodes")
        
        # Fusion nodes (extracted from features)
        fusion_count = 0
        for fusion_id, fusion in self.fusions.items():
            fusion_count += 1
            yield (fusion_id, "fusion", fusion)
        
        self.logger.info(f"Generated {fusion_count} fusion nodes")
        
        # Feature nodes
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
        
        # 0. Gene → Feature edges
        for feature_id, feature in self.features.items():
            gene_name = feature.get("name", "").strip()
            feature_type = feature.get("feature_type", "").lower()
            
            # Only create edge if this feature has a corresponding gene
            if feature_type == "gene" and gene_name:
                gene_id = gene_name
                if gene_id in self.genes:
                    edge_counter += 1
                    yield (
                        f"civic_edge_{edge_counter}",
                        gene_id,
                        feature_id,
                        "HAS_FEATURE",
                        {"data_source": "CIViC", "relationship_type": "gene_to_feature"}
                    )
        
        # 0b. Fusion → Feature edges
        for feature_id, feature in self.features.items():
            fusion_name = feature.get("name", "").strip()
            feature_type = feature.get("feature_type", "").lower()
            
            # Only create edge if this feature has a corresponding fusion
            if feature_type == "fusion" and fusion_name:
                fusion_id = fusion_name
                if fusion_id in self.fusions:
                    edge_counter += 1
                    yield (
                        f"civic_edge_{edge_counter}",
                        fusion_id,
                        feature_id,
                        "HAS_FEATURE",
                        {"data_source": "CIViC", "relationship_type": "fusion_to_feature"}
                    )
        
        # 1. Feature → Variant edges
        for variant_id, variant in self.variants.items():
            if variant.get("feature_id"):
                # Ensure feature_id is in full format
                feature_id_raw = variant["feature_id"]
                feature_id_full = f"civic_feature:{feature_id_raw}" if not feature_id_raw.startswith("civic_feature:") else feature_id_raw
                
                edge_counter += 1
                yield (
                    f"civic_edge_{edge_counter}",
                    feature_id_full,
                    variant_id,
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
                    therapy_id = therapy_name.strip()  # Use therapy name directly as ID
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
                    therapy_id = therapy_name.strip()  # Use therapy name directly as ID
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
        self.logger.info(f"Parsed {len(self.features)} features")
        self.logger.info(f"Extracted {len(self.genes)} genes from features")
        self.logger.info(f"Extracted {len(self.fusions)} fusions from features")
        self.logger.info(f"Parsed {len(self.variant_groups)} variant groups")
        self.logger.info(f"Parsed {len(self.variants)} variants")
        self.logger.info(f"Parsed {len(self.molecular_profiles)} molecular profiles")
        self.logger.info(f"Parsed {len(self.evidence_items)} evidence items")
        self.logger.info(f"Parsed {len(self.assertions)} assertions")
        self.logger.info(f"Extracted {len(self.diseases)} diseases")
        self.logger.info(f"Extracted {len(self.therapies)} therapies")
