import random
import string
import sys
import os
import hashlib
from enum import Enum, auto
from itertools import chain
from typing import Optional
from biocypher._logger import logger

# Add path for utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

logger.debug(f"Loading module {__name__}.")

import requests

# Default query parameters - can be overridden by config
DEFAULT_QUERY_PARAMS = {
    "format": "json",
    "query.cond": "iga nephropathy",
    # "query.parser": "advanced",
    # "query.term": "AREA[LastUpdatePostDate]RANGE[2023-01-15,MAX]",
    # "query.locn": "",
    # "query.titles": "",
    # "query.intr": "",
    # "query.outc": "",
    # "query.spons": "",
    # "query.lead": "",
    # "query.id": "",
    # "query.patient": "",
    # "filter.overallStatus": ["NOT_YET_RECRUITING", "RECRUITING"],
    # "filter.geo": "",
    # "filter.ids": ["NCT04852770", "NCT01728545", "NCT02109302"],
    # "filter.advanced": "",
    # "filter.synonyms": "",
    # "postFilter.overallStatus": ["NOT_YET_RECRUITING", "RECRUITING"],
    # "postFilter.geo": "",
    # "postFilter.ids": ["NCT04852770", "NCT01728545", "NCT02109302"],
    # "postFilter.advanced": "",
    # "postFilter.synonyms": "",
    # "aggFilters": "",
    # "geoDecay": "",
    # "fields": ["NCTId", "BriefTitle", "OverallStatus", "HasResults"],
    # "sort": ["@relevance"],
    # "countTotal": False,
    # "pageSize": 10,
    # "pageToken": "",
}


class ClinicalTrialsAdapterNodeType(Enum):
    """
    Define types of nodes the adapter can provide.
    """

    STUDY = auto()
    ORGANISATION = auto()
    SPONSOR = auto()
    OUTCOME = auto()
    DRUG = auto()
    DISEASE = auto()
    LOCATION = auto()


class ClinicalTrialsAdapterStudyField(Enum):
    """
    Define possible fields the adapter can provide for studies.
    """

    ID = "identificationModule/nctId"
    BRIEF_TITLE = "identificationModule/briefTitle"
    OFFICIAL_TITLE = "identificationModule/officialTitle"
    STATUS = "statusModule/overallStatus"
    BRIEF_SUMMARY = "descriptionModule/briefSummary"
    TYPE = "designModule/studyType"
    ALLOCATION = "designModule/designInfo/allocation"
    PHASES = "designModule/phases"
    MODEL = "designModule/designInfo/interventionModel"
    PRIMARY_PURPOSE = "designModule/designInfo/primaryPurpose"
    NUMBER_OF_PATIENTS = "designModule/enrollmentInfo/count"
    ELIGIBILITY_CRITERIA = "eligibilityModule/eligibilityCriteria"
    HEALTHY_VOLUNTEERS = "eligibilityModule/healthyVolunteers"
    SEX = "eligibilityModule/sex"
    MINIMUM_AGE = "eligibilityModule/minimumAge"
    MAXIMUM_AGE = "eligibilityModule/maximumAge"
    STANDARDISED_AGES = "eligibilityModule/stdAges"


class ClinicalTrialsAdapterDiseaseField(Enum):
    """
    Define possible fields the adapter can provide for diseases.
    """

    ID = "id"
    NAME = "name"
    DESCRIPTION = "description"


class ClinicalTrialsAdapterEdgeType(Enum):
    """
    Enum for the types of the protein adapter.
    """

    STUDY_TO_DRUG = auto()
    STUDY_TO_DISEASE = auto()
    STUDY_TO_LOCATION = auto()
    STUDY_TO_SPONSOR = auto()
    STUDY_TO_OUTCOME = auto()


class ClinicalTrialsAdapterProteinProteinEdgeField(Enum):
    """
    Define possible fields the adapter can provide for protein-protein edges.
    """

    INTERACTION_TYPE = "interaction_type"
    INTERACTION_SOURCE = "interaction_source"


class ClinicalTrialsAdapterProteinDiseaseEdgeField(Enum):
    """
    Define possible fields the adapter can provide for protein-disease edges.
    """

    ASSOCIATION_TYPE = "association_type"
    ASSOCIATION_SOURCE = "association_source"


class ClinicalTrialsAdapter:
    """
    ClinicalTrials BioCypher adapter. Generates nodes and edges for creating a
    knowledge graph.

    Args:
        node_types: List of node types to include in the result.
        node_fields: List of node fields to include in the result.
        edge_types: List of edge types to include in the result.
        edge_fields: List of edge fields to include in the result.
    """

    def __init__(
        self,
        node_types: Optional[list] = None,
        node_fields: Optional[list] = None,
        edge_types: Optional[list] = None,
        edge_fields: Optional[list] = None,
        config: Optional[dict] = None,
    ):
        self._set_types_and_fields(
            node_types, node_fields, edge_types, edge_fields
        )

        self.base_url = "https://clinicaltrials.gov/api/v2"

        # Get query parameters from config or use defaults
        if config and config.get('clinical_trials', {}).get('query_params'):
            query_params = config['clinical_trials']['query_params']
            logger.info(f"Using query parameters from config: {query_params}")
        else:
            query_params = DEFAULT_QUERY_PARAMS
            logger.info(f"Using default query parameters: {query_params}")

        # Get max_studies parameter from config
        max_studies = None
        if config and config.get('clinical_trials', {}).get('max_studies'):
            max_studies = config['clinical_trials']['max_studies']
            logger.info(f"Max studies limit set to: {max_studies}")
        else:
            logger.info("No max_studies limit set - will fetch all available studies")

        self._studies = self._get_studies(query_params, max_studies)

        self._preprocess()

    def _generate_edge_id(self, source_id: str, target_id: str, edge_type: str, properties: dict = None) -> str:
        """
        Generate a unique edge ID based on source, target, edge type, and properties.
        
        Args:
            source_id: Source node ID
            target_id: Target node ID  
            edge_type: Type of edge
            properties: Edge properties (optional)
            
        Returns:
            Unique edge ID string
        """
        # Create a string to hash that includes all identifying information
        id_components = [source_id, target_id, edge_type]
        
        # Add relevant properties to make edges unique
        if properties:
            # For outcome edges, include primary status to distinguish primary vs secondary
            if edge_type == "study_has_outcome" and "primary" in properties:
                id_components.append(f"primary_{properties['primary']}")
            
            # Add description if available to further distinguish edges
            if "description" in properties and properties["description"] != "N/A":
                id_components.append(properties["description"][:50])  # Limit length
        
        # Create hash of components
        id_string = "|".join(str(comp) for comp in id_components)
        edge_hash = hashlib.md5(id_string.encode()).hexdigest()[:12]  # Use first 12 chars
        
        # Create readable edge ID
        edge_id = f"{edge_type}_{edge_hash}"
        
        return edge_id

    def _get_studies(self, query_params, max_studies=None):
        """
        Get all studies fitting the parameters from the API.
        Handles large-scale data fetching with proper pagination and memory management.

        Args:
            query_params: Dictionary of query parameters to pass to the API.

        Returns:
            A list of studies (dictionaries).
        """
        try:
            url = f"{self.base_url}/studies"
            logger.info(f"Fetching studies from: {url}")
            
            # Clean up query parameters for API compatibility
            clean_params = query_params.copy()
            
            # Handle fields parameter properly - ClinicalTrials API expects comma-separated values
            if 'fields' in clean_params and isinstance(clean_params['fields'], list):
                clean_params['fields'] = ','.join(clean_params['fields'])
                logger.info(f"Converted fields to comma-separated: {clean_params['fields']}")
            
            logger.info(f"Clean query parameters: {clean_params}")
            
            # Don't try to get total count first - it's causing API errors
            # Just start fetching data directly
            logger.info("Starting data fetch...")
            response = requests.get(url, params=clean_params, timeout=30)
            response.raise_for_status()
            
            result = response.json()
            all_studies = result.get("studies", [])
            logger.info(f"Initial batch: {len(all_studies)} studies")
            
            if len(all_studies) == 0:
                logger.warning("No studies returned in first batch")
                return []
            
            # If max_studies is set and initial batch exceeds it, truncate
            if max_studies and len(all_studies) > max_studies:
                all_studies = all_studies[:max_studies]
                logger.info(f"Truncated initial batch to {max_studies} studies (max_studies limit)")
                return all_studies
            
            # Handle pagination with progress tracking
            page_count = 1
            max_pages = 1000  # Safety limit - adjust based on needs
            
            while result.get("nextPageToken") and page_count < max_pages:
                # Check if we've reached the max_studies limit
                if max_studies and len(all_studies) >= max_studies:
                    logger.info(f"Reached max_studies limit of {max_studies:,}, stopping pagination")
                    break
                
                clean_params["pageToken"] = result.get("nextPageToken")
                page_count += 1
                
                logger.info(f"Fetching page {page_count}... ({len(all_studies):,} studies so far)")
                
                response = requests.get(url, params=clean_params, timeout=30)
                response.raise_for_status()
                
                next_page = response.json()
                if next_page.get("studies"):
                    new_studies = next_page.get("studies")
                    
                    # If adding all new studies would exceed max_studies, only add what we need
                    if max_studies and len(all_studies) + len(new_studies) > max_studies:
                        remaining_needed = max_studies - len(all_studies)
                        new_studies = new_studies[:remaining_needed]
                        logger.info(f"Limiting to {remaining_needed} studies to stay within max_studies limit")
                    
                    all_studies.extend(new_studies)
                    logger.info(f"Added {len(new_studies)} studies from page {page_count}")
                
                result["nextPageToken"] = next_page.get("nextPageToken")
                
                # Progress update every 10 pages
                if page_count % 10 == 0:
                    logger.info(f"Progress: {len(all_studies):,} studies fetched in {page_count} pages")
                
                # Memory management for very large datasets
                if len(all_studies) > 50000 and page_count % 50 == 0:
                    logger.info("Large dataset - consider processing in batches for better memory management")
            
            if page_count >= max_pages and result.get("nextPageToken"):
                logger.warning(f"Reached page limit ({max_pages}), stopping pagination")
                logger.warning(f"Fetched {len(all_studies):,} studies")
                logger.warning("Increase max_pages limit or add filters to get all data")
            
            total_fetched = len(all_studies)
            if max_studies and total_fetched >= max_studies:
                logger.info(f"Data fetch complete: {total_fetched:,} studies fetched (limited by max_studies={max_studies:,})")
            else:
                logger.info(f"Data fetch complete: {total_fetched:,} studies fetched in {page_count} pages")
            
            return all_studies
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            logger.error(f"Failed URL: {e.response.url if hasattr(e, 'response') and e.response else 'Unknown'}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response text: {e.response.text[:500]}...")
            return []
        except Exception as e:
            logger.error(f"Error processing API response: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _preprocess(self):
        """
        Preprocess raw API results into node and edge types.
        """

        self._organisations = {}
        self._sponsors = {}
        self._outcomes = {}
        self._interventions = {}
        self._diseases = {}
        self._locations = {}

        self._study_to_drug_edges = []
        self._study_to_disease_edges = []
        self._study_to_location_edges = []
        self._study_to_sponsor_edges = []
        self._study_to_outcome_edges = []

        for study in self._studies:
            self._preprocess_study(study)

    def _preprocess_study(self, study: dict):
        if not study.get("protocolSection"):
            return

        try:
            _id = (
                study.get("protocolSection")
                .get("identificationModule")
                .get("nctId")
            )
        except AttributeError:
            _id = None

        if not _id:
            return

        study["nctId"] = _id

        protocol = study.get("protocolSection")
        # the derived module has interesting info about conditions and
        # interventions, linking to MeSH terms; could use for diseases and
        # drugs

        # Initialize variables
        name = None
        oclass = None

        # organisations
        if ClinicalTrialsAdapterNodeType.ORGANISATION in self.node_types:
            try:
                name = (
                    protocol.get("identificationModule")
                    .get("organization")
                    .get("fullName")
                )
            except AttributeError:
                name = None

            try:
                oclass = (
                    protocol.get("identificationModule")
                    .get("organization")
                    .get("class")
                )
            except AttributeError:
                oclass = None

            if name:
                if name not in self._organisations:
                    self._organisations.update(
                        {
                            name: {"class": oclass or "N/A"},
                        }
                    )

        # sponsor
        if ClinicalTrialsAdapterNodeType.SPONSOR in self.node_types:
            try:
                lead = protocol.get("sponsorCollaboratorsModule").get(
                    "leadSponsor"
                )
            except AttributeError:
                lead = None

            if lead:
                name = lead.get("name")
                
                # Normalize Unicode characters to avoid encoding issues
                if name:
                    name = normalize_unicode(name)

                if name not in self._sponsors.keys():
                    self._sponsors.update(
                        {
                            name: {
                                "class": lead.get("class"),
                            }
                        }
                    )

                # study to sponsor edges
                edge_properties = {"data_source": "ClinicalTrials.gov"}
                edge_id = self._generate_edge_id(_id, name, "study_has_sponsor", edge_properties)
                
                self._study_to_sponsor_edges.append(
                    (
                        edge_id,
                        _id,
                        name,
                        "study_has_sponsor",
                        edge_properties,
                    )
                )

        # outcomes
        if ClinicalTrialsAdapterNodeType.OUTCOME in self.node_types:
            try:
                primary = protocol.get("outcomesModule").get("primaryOutcomes")
            except AttributeError:
                primary = None

            try:
                secondary = protocol.get("outcomesModule").get(
                    "secondaryOutcomes"
                )
            except AttributeError:
                secondary = None

            if primary:
                for outcome in primary:
                    self._add_outcome(outcome, True, _id)

            if secondary:
                for outcome in secondary:
                    self._add_outcome(outcome, False, _id)

        # drugs
        if ClinicalTrialsAdapterNodeType.DRUG in self.node_types:
            try:
                interventions = protocol.get("armsInterventionsModule").get(
                    "interventions"
                )
            except AttributeError:
                interventions = None

            if interventions:
                for intervention in interventions:
                    try:
                        name = intervention.get("name")
                        # Normalize Unicode characters to avoid encoding issues
                        if name:
                            name = normalize_unicode(name)
                    except AttributeError:
                        name = None

                    try:
                        intervention_type = intervention.get("type")
                    except AttributeError:
                        intervention_type = None

                    try:
                        description = intervention.get("description")
                        if description:
                            description = normalize_unicode(description)
                            description = replace_quote(description)
                            description = replace_newline(description)
                    except AttributeError:
                        description = None

                    try:
                        mapped_names = intervention.get(
                            "interventionMappedName"
                        )
                    except AttributeError:
                        mapped_names = None

                    if name:
                        if name not in self._interventions.keys():
                            self._interventions.update(
                                {
                                    name: {
                                        "type": intervention_type or "N/A",
                                        "description": description or "N/A",
                                        "mapped_names": mapped_names or "N/A",
                                    },
                                }
                            )

                        # study to drug edges
                        if str(intervention_type).lower() == "drug":
                            edge_properties = {
                                "description": description or "N/A",
                                "data_source": "ClinicalTrials.gov"
                            }
                            edge_id = self._generate_edge_id(_id, name, "study_has_drug", edge_properties)
                            
                            self._study_to_drug_edges.append(
                                (
                                    edge_id,
                                    _id,
                                    name,
                                    "study_has_drug",
                                    edge_properties,
                                )
                            )

        # diseases
        if ClinicalTrialsAdapterNodeType.DISEASE in self.node_types:
            try:
                conditions = protocol.get("conditionsModule").get("conditions")
            except AttributeError:
                conditions = None

            try:
                keywords = protocol.get("conditionsModule").get("keywords")
            except AttributeError:
                keywords = []

            if conditions:
                for condition in conditions:
                    # Use condition name as ID (simple replacement for normalize_disease_id)
                    normalized_id = condition.strip().replace(" ", "_").replace("-", "_").upper() if condition else None
                    
                    if normalized_id:  # Only process if we have a valid condition
                        if normalized_id not in self._diseases.keys():
                            self._diseases[normalized_id] = {
                                "name": condition,  # Keep original name
                                "original_id": condition,  # Store original as separate property
                                "keywords": keywords if keywords else []
                            }
                        else:
                            # Merge keywords if disease already exists
                            if keywords:
                                existing_keywords = self._diseases[normalized_id].get("keywords", [])
                                if existing_keywords:
                                    # Extend with new keywords, avoiding duplicates
                                    combined_keywords = list(set(existing_keywords + keywords))
                                    self._diseases[normalized_id]["keywords"] = combined_keywords
                                else:
                                    self._diseases[normalized_id]["keywords"] = keywords

                        # study to disease edges (use normalized ID)
                        edge_properties = {"data_source": "ClinicalTrials.gov"}
                        edge_id = self._generate_edge_id(_id, normalized_id, "study_has_disease", edge_properties)
                        
                        self._study_to_disease_edges.append(
                            (
                                edge_id,
                                _id,
                                normalized_id,  # Use normalized ID for edge target
                                "study_has_disease",
                                edge_properties,
                            )
                        )

        # locations
        if ClinicalTrialsAdapterNodeType.LOCATION in self.node_types:
            if not protocol.get("contactsLocationsModule"):
                return  # only works in last position of flow?
            if not protocol.get("contactsLocationsModule").get("locations"):
                return

            for location in protocol.get("contactsLocationsModule").get(
                "locations"
            ):
                try:
                    facility = location.get("facility")
                    city = location.get("city") 
                    country = location.get("country")
                    
                    # Normalize Unicode characters and apply other cleaning
                    if facility:
                        facility = normalize_unicode(replace_quote(facility))
                    if city:
                        city = normalize_unicode(replace_quote(city))
                    if country:
                        country = normalize_unicode(replace_quote(country))
                    
                    name = ", ".join([
                        facility or "",
                        city or "",
                        country or ""
                    ]).strip(", ")
                except AttributeError:
                    name = None
                    facility = None
                    city = None
                    country = None

                # Individual field processing for backward compatibility
                try:
                    city = normalize_unicode(replace_quote(location.get("city"))) if location.get("city") else None
                except AttributeError:
                    city = None

                try:
                    state = normalize_unicode(replace_quote(location.get("state"))) if location.get("state") else None
                except AttributeError:
                    state = None

                try:
                    country = normalize_unicode(replace_quote(location.get("country"))) if location.get("country") else None
                except AttributeError:
                    country = None

                if name:
                    if name not in self._locations.keys():
                        self._locations.update(
                            {
                                name: {
                                    "city": city or "N/A",
                                    "state": state or "N/A",
                                    "country": country or "N/A",
                                },
                            }
                        )

                    # study to location edges
                    edge_properties = {"data_source": "ClinicalTrials.gov"}
                    edge_id = self._generate_edge_id(_id, name, "study_has_location", edge_properties)
                    
                    self._study_to_location_edges.append(
                        (
                            edge_id,
                            _id,
                            name,
                            "study_has_location",
                            edge_properties,
                        )
                    )

    def _add_outcome(self, outcome: dict, primary: bool, study_id: str):
        try:
            measure = outcome.get("measure")
            measure = replace_quote(measure)
        except AttributeError:
            measure = None

        try:
            time_frame = outcome.get("timeFrame")
        except AttributeError:
            time_frame = None

        try:
            description = outcome.get("description")
            description = replace_quote(description)
        except AttributeError:
            description = None

        if measure:
            # Handle outcome node creation/updating
            if measure not in self._outcomes:
                # First time seeing this outcome measure
                self._outcomes[measure] = {
                    "primary": primary,
                    "time_frame": time_frame or "N/A",
                    "description": description or "N/A",
                }
            else:
                # Outcome already exists - update if this instance is primary
                # (prioritize primary outcomes over secondary ones)
                if primary and not self._outcomes[measure]["primary"]:
                    self._outcomes[measure]["primary"] = True
                
                # Update description and time_frame if current ones are "N/A" and we have better data
                if self._outcomes[measure]["description"] == "N/A" and description:
                    self._outcomes[measure]["description"] = description
                if self._outcomes[measure]["time_frame"] == "N/A" and time_frame:
                    self._outcomes[measure]["time_frame"] = time_frame
            
            # Always create study to outcome edge
            edge_properties = {
                "primary": primary,
                "time_frame": time_frame or "N/A",
                "description": description or "N/A",
                "data_source": "ClinicalTrials.gov"
            }
            edge_id = self._generate_edge_id(study_id, measure, "study_has_outcome", edge_properties)
            
            self._study_to_outcome_edges.append(
                (
                    edge_id,
                    study_id,
                    measure,
                    "study_has_outcome",
                    edge_properties,
                )
            )

    def get_nodes(self):
        """
        Returns a generator of node tuples for node types specified in the
        adapter constructor.
        """

        logger.info("Generating nodes.")

        if ClinicalTrialsAdapterNodeType.STUDY in self.node_types:
            for study in self._studies:
                if not study.get("nctId"):
                    continue

                _props = self._get_study_props_from_fields(study)
                _props['data_source'] = 'ClinicalTrials.gov'

                yield (study.get("nctId"), "study", _props)

        # if ClinicalTrialsAdapterNodeType.ORGANISATION in self.node_types:
        #     for name, props in self._organisations.items():
        #         yield (name, "organisation", props)

        if ClinicalTrialsAdapterNodeType.SPONSOR in self.node_types:
            for name, props in self._sponsors.items():
                # Create a copy of props to avoid modifying the original
                node_props = props.copy()
                node_props['data_source'] = 'ClinicalTrials.gov'
                yield (name, "sponsor", node_props)

        if ClinicalTrialsAdapterNodeType.OUTCOME in self.node_types:
            for measure, props in self._outcomes.items():
                # Create a copy of props to avoid modifying the original
                node_props = props.copy()
                node_props['data_source'] = 'ClinicalTrials.gov'
                yield (measure, "outcome", node_props)

        if ClinicalTrialsAdapterNodeType.DRUG in self.node_types:
            for name, props in self._interventions.items():
                # Create a copy of props to avoid modifying the original
                node_props = props.copy()
                node_props['data_source'] = 'ClinicalTrials.gov'
                yield (name, "drug", node_props)

        if ClinicalTrialsAdapterNodeType.DISEASE in self.node_types:
            for normalized_id, props in self._diseases.items():
                # Create a copy of props to avoid modifying the original
                node_props = props.copy()
                node_props['data_source'] = 'ClinicalTrials.gov'
                node_props['id'] = normalized_id  # Ensure ID is set
                yield (normalized_id, "disease", node_props)

        if ClinicalTrialsAdapterNodeType.LOCATION in self.node_types:
            for name, props in self._locations.items():
                # Create a copy of props to avoid modifying the original
                node_props = props.copy()
                node_props['data_source'] = 'ClinicalTrials.gov'
                yield (name, "location", node_props)

    def _get_study_props_from_fields(self, study):
        """
        Returns a dictionary of properties for a study node, given the selected
        fields.

        Args:
            study: The study (raw API result) to extract properties from.

        Returns:
            A dictionary of properties.
        """

        props = {}

        for field in self.node_fields:
            if field not in ClinicalTrialsAdapterStudyField:
                continue

            if field == ClinicalTrialsAdapterStudyField.ID:
                continue

            path = field.value.split("/")
            value = study.get("protocolSection")

            if value:
                for step in path:
                    if value:
                        value = value.get(step)

            if isinstance(value, list):
                value = [replace_quote(v) for v in value]
            elif isinstance(value, str):
                value = replace_quote(value)

            props.update({field.name.lower(): value or "N/A"})

        return props

    def get_edges(self):
        """
        Returns a generator of edge tuples for edge types specified in the
        adapter constructor.
        """

        logger.info("Generating edges.")

        if ClinicalTrialsAdapterEdgeType.STUDY_TO_DRUG in self.edge_types:
            yield from self._study_to_drug_edges

        if ClinicalTrialsAdapterEdgeType.STUDY_TO_DISEASE in self.edge_types:
            yield from self._study_to_disease_edges

        if ClinicalTrialsAdapterEdgeType.STUDY_TO_LOCATION in self.edge_types:
            yield from self._study_to_location_edges

        if ClinicalTrialsAdapterEdgeType.STUDY_TO_SPONSOR in self.edge_types:
            yield from self._study_to_sponsor_edges

        if ClinicalTrialsAdapterEdgeType.STUDY_TO_OUTCOME in self.edge_types:
            yield from self._study_to_outcome_edges

    def _set_types_and_fields(
        self, node_types, node_fields, edge_types, edge_fields
    ):
        if node_types:
            self.node_types = node_types
        else:
            self.node_types = [type for type in ClinicalTrialsAdapterNodeType]

        if node_fields:
            self.node_fields = node_fields
        else:
            self.node_fields = [
                field
                for field in chain(
                    ClinicalTrialsAdapterStudyField,
                    ClinicalTrialsAdapterDiseaseField,
                )
            ]

        if edge_types:
            self.edge_types = edge_types
        else:
            self.edge_types = [type for type in ClinicalTrialsAdapterEdgeType]

        if edge_fields:
            self.edge_fields = edge_fields
        else:
            self.edge_fields = [field for field in chain()]


def replace_quote(string):
    return string.replace('"', "'")


def replace_newline(string):
    return string.replace("\n", " | ")


def normalize_unicode(string):
    """
    Normalize Unicode characters to ASCII equivalents to avoid encoding issues
    """
    if not string:
        return string
    
    # Common Unicode character mappings
    unicode_map = {
        # Basic Latin accented characters
        'á': 'a', 'à': 'a', 'ä': 'a', 'â': 'a', 'ã': 'a', 'å': 'a',
        'é': 'e', 'è': 'e', 'ë': 'e', 'ê': 'e',
        'í': 'i', 'ì': 'i', 'ï': 'i', 'î': 'i',
        'ó': 'o', 'ò': 'o', 'ö': 'o', 'ô': 'o', 'õ': 'o',
        'ú': 'u', 'ù': 'u', 'ü': 'u', 'û': 'u',
        'ñ': 'n', 'ç': 'c',
        'Á': 'A', 'À': 'A', 'Ä': 'A', 'Â': 'A', 'Ã': 'A', 'Å': 'A',
        'É': 'E', 'È': 'E', 'Ë': 'E', 'Ê': 'E',
        'Í': 'I', 'Ì': 'I', 'Ï': 'I', 'Î': 'I',
        'Ó': 'O', 'Ò': 'O', 'Ö': 'O', 'Ô': 'O', 'Õ': 'O',
        'Ú': 'U', 'Ù': 'U', 'Ü': 'U', 'Û': 'U',
        'Ñ': 'N', 'Ç': 'C',
        
        # Extended Latin characters (Turkish, Polish, etc.)
        'ş': 's', 'Ş': 'S',  # Turkish s with cedilla
        'ğ': 'g', 'Ğ': 'G',  # Turkish g with breve
        'ı': 'i', 'İ': 'I',  # Turkish dotless i / i with dot
        'ž': 'z', 'Ž': 'Z',  # z with caron
        'ł': 'l', 'Ł': 'L',  # Polish l with stroke
        'ć': 'c', 'Ć': 'C',  # c with acute
        'ś': 's', 'Ś': 'S',  # s with acute
        'ź': 'z', 'Ź': 'Z',  # z with acute
        'ż': 'z', 'Ż': 'Z',  # z with dot above
        'ń': 'n', 'Ń': 'N',  # n with acute
        'ř': 'r', 'Ř': 'R',  # r with caron
        'š': 's', 'Š': 'S',  # s with caron
        'č': 'c', 'Č': 'C',  # c with caron
        'ď': 'd', 'Ď': 'D',  # d with caron
        'ť': 't', 'Ť': 'T',  # t with caron
        'ň': 'n', 'Ň': 'N',  # n with caron
        'ů': 'u', 'Ů': 'U',  # u with ring above
        'ē': 'e', 'Ē': 'E',  # e with macron
        'ī': 'i', 'Ī': 'I',  # i with macron
        'ā': 'a', 'Ā': 'A',  # a with macron
        'ō': 'o', 'Ō': 'O',  # o with macron
        'ū': 'u', 'Ū': 'U',  # u with macron
        
        # Special symbols and punctuation
        '®': '',              # Registered trademark (remove)
        '™': '',              # Trademark (remove)
        '©': '',              # Copyright (remove)
        '•': '-',             # Bullet point -> dash
        '–': '-',             # En dash -> hyphen
        '—': '-',             # Em dash -> hyphen
        ''': "'",             # Left single quotation mark
        ''': "'",             # Right single quotation mark
        '"': '"',             # Left double quotation mark
        '"': '"',             # Right double quotation mark
        '…': '...',           # Horizontal ellipsis
        
        # Chinese/CJK punctuation
        '，': ',',             # Chinese comma
        '。': '.',             # Chinese period
        '（': '(',             # Chinese left parenthesis
        '）': ')',             # Chinese right parenthesis
        '：': ':',             # Chinese colon
        '；': ';',             # Chinese semicolon
        '？': '?',             # Chinese question mark
        '！': '!',             # Chinese exclamation mark
    }
    
    # Replace Unicode characters with ASCII equivalents
    normalized = string
    for unicode_char, ascii_char in unicode_map.items():
        normalized = normalized.replace(unicode_char, ascii_char)
    
    return normalized