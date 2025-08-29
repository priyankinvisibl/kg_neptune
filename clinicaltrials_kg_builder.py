#!/usr/bin/env python3
"""
ClinicalTrials.gov Knowledge Graph Builder
Builds knowledge graphs from ClinicalTrials.gov data with configurable properties
"""

import os
import sys
import time
import argparse
import yaml
import requests
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from biocypher import BioCypher
from utils.filehandler import FileHandler
from adapters.clinicalTrials.clinicaltrials_adapter import ClinicalTrialsAdapter
from utils.neptune_converter import convert_to_neptune

def load_clinical_trials_config(config_path="/app/config/kg_config_clinicaltrials.yaml"):
    """
    Load ClinicalTrials configuration from a YAML file
    
    Args:
        config_path: Path to the ClinicalTrials config file
        
    Returns:
        Dictionary containing ClinicalTrials configuration
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config
    
    except Exception as e:
        logger.error(f"Error loading ClinicalTrials config: {e}")
        import traceback
        traceback.print_exc()
        return {}

def get_clinical_trials_data_files(config, main_config=None):
    """
    Get ClinicalTrials data files from configuration
    
    Args:
        config: ClinicalTrials configuration dictionary
        main_config: Main configuration dictionary (optional)
        
    Returns:
        Dictionary of data files
    """
    files = {}
    
    # Handle URL-based configuration (preferred)
    if main_config and 'datasets' in main_config and 'clinicaltrials' in main_config['datasets']:
        ct_config = main_config['datasets']['clinicaltrials']
        
        if 'api_config' in ct_config:
            # API-based configuration
            files['api_config'] = ct_config['api_config']
            logger.info("Using API-based ClinicalTrials configuration")
        elif 'file_path' in ct_config:
            # File-based configuration
            files['clinical_trials'] = ct_config['file_path']
            logger.info(f"Using file-based ClinicalTrials configuration: {ct_config['file_path']}")
    
    # Fallback to file-based configuration
    if 'clinical_trials' in config:
        files['clinical_trials'] = config['clinical_trials'].get('file_path', 'clinicaltrials/clinical_trials.json')
    
    return files

def download_clinical_trials_file(url, local_path):
    """
    Download a ClinicalTrials file from URL to local path
    
    Args:
        url: URL to download from
        local_path: Local path to save to
        
    Returns:
        Boolean indicating success
    """
    try:
        logger.info(f"Downloading {url} to {local_path}")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Successfully downloaded {local_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        return False

def create_clinical_trials_adapters(data_files, main_config=None):
    """
    Create ClinicalTrials adapters based on available data files
    
    Args:
        data_files: Dictionary of data file paths
        main_config: Main configuration dictionary
        
    Returns:
        List of adapter instances
    """
    adapters = []
    
    # Check if we have API configuration in the main config
    if main_config and 'clinical_trials' in main_config:
        logger.info("Creating ClinicalTrialsAdapter with API configuration from main config")
        
        try:
            # Import the adapter and its enums
            from adapters.clinicalTrials.clinicaltrials_adapter import (
                ClinicalTrialsAdapter, 
                ClinicalTrialsAdapterNodeType,
                ClinicalTrialsAdapterStudyField,
                ClinicalTrialsAdapterDiseaseField,
                ClinicalTrialsAdapterEdgeType
            )
        except ImportError as e:
            logger.error(f"Failed to import ClinicalTrials adapter: {e}")
            return adapters
        
        # Extract node and edge configuration if available
        ct_config = main_config['clinical_trials']
        
        # Convert string node types to enum values
        node_types = None
        if 'node_types' in ct_config:
            node_types = []
            for node_type_str in ct_config['node_types']:
                try:
                    enum_value = getattr(ClinicalTrialsAdapterNodeType, node_type_str)
                    node_types.append(enum_value)
                    logger.info(f"Added node type: {node_type_str}")
                except AttributeError:
                    logger.warning(f"Unknown node type: {node_type_str}")
        
        # Convert string node fields to enum values
        node_fields = None
        if 'node_fields' in ct_config:
            node_fields = []
            for field_str in ct_config['node_fields']:
                # Try both Study and Disease field enums
                found = False
                for enum_class in [ClinicalTrialsAdapterStudyField, ClinicalTrialsAdapterDiseaseField]:
                    try:
                        enum_value = getattr(enum_class, field_str)
                        node_fields.append(enum_value)
                        logger.info(f"Added node field: {field_str}")
                        found = True
                        break
                    except AttributeError:
                        continue
                if not found:
                    logger.warning(f"Unknown node field: {field_str}")
        
        # Convert string edge types to enum values
        edge_types = None
        if 'edge_types' in ct_config:
            edge_types = []
            for edge_type_str in ct_config['edge_types']:
                try:
                    enum_value = getattr(ClinicalTrialsAdapterEdgeType, edge_type_str)
                    edge_types.append(enum_value)
                    logger.info(f"Added edge type: {edge_type_str}")
                except AttributeError:
                    logger.warning(f"Unknown edge type: {edge_type_str}")
        
        edge_fields = ct_config.get('edge_fields')
        
        logger.info(f"Creating adapter with {len(node_types) if node_types else 0} node types, {len(node_fields) if node_fields else 0} node fields")
        
        # Create adapter with config
        adapter = ClinicalTrialsAdapter(
            node_types=node_types,
            node_fields=node_fields,
            edge_types=edge_types,
            edge_fields=edge_fields,
            config=main_config
        )
        adapters.append(adapter)
        return adapters
    
    # Handle URL-based configuration (preferred)
    if main_config and 'datasets' in main_config and 'clinicaltrials' in main_config['datasets']:
        ct_config = main_config['datasets']['clinicaltrials']
        
        if 'api_config' in ct_config:
            # Create adapter with API configuration
            logger.info("Creating ClinicalTrialsAdapter with API configuration")
            adapter = ClinicalTrialsAdapter(config={'clinical_trials': ct_config['api_config']})
            adapters.append(adapter)
            return adapters
        
        # Handle URL downloads
        for dataset_name, dataset_config in ct_config.items():
            if isinstance(dataset_config, dict) and 'url' in dataset_config:
                url = dataset_config['url']
                local_path = f"/app/data/clinicaltrials/{dataset_name}.json"
                
                if download_clinical_trials_file(url, local_path):
                    # Create appropriate adapter based on dataset name
                    if dataset_name == 'clinical_trials':
                        logger.info(f"Creating ClinicalTrialsAdapter for {local_path}")
                        adapters.append(ClinicalTrialsAdapter(config={'file_path': local_path}))
                    else:
                        logger.warning(f"Unknown ClinicalTrials dataset: {dataset_name}")
                else:
                    logger.error(f"Failed to download {dataset_name} from {url}")
    
    else:
        # Handle file-based configuration (original logic)
        if 'clinical_trials' in data_files:
            file_path = data_files['clinical_trials']
            if os.path.exists(file_path):
                logger.info(f"Creating ClinicalTrialsAdapter for {file_path}")
                adapters.append(ClinicalTrialsAdapter(config={'file_path': file_path}))
            else:
                logger.warning(f"ClinicalTrials file not found: {file_path}")
    
    return adapters

def build_clinical_trials_knowledge_graph(config_path="/app/config/kg_config_clinicaltrials.yaml", 
                                         schema_config_path="/app/config/schema_clinical_trial.yaml",
                                         output_dir="/app/output",
                                         convert_to_neptune_format=True,
                                         config=None):
    """
    Build ClinicalTrials knowledge graph
    
    Args:
        config_path: Path to the main config file
        schema_config_path: Path to the schema config file
        output_dir: Output directory for generated files
        convert_to_neptune_format: Whether to convert to Neptune format
        config: Optional config dictionary (overrides config_path if provided)
    """
    
    logger.info("=" * 80)
    logger.info("CLINICALTRIALS KNOWLEDGE GRAPH BUILDER")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    try:
        # Use provided config or load from file
        if config is not None:
            main_config = config
            logger.info("Using provided config dictionary")
        else:
            # Load main configuration from file
            main_config = {}
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    main_config = yaml.safe_load(f)
                logger.info(f"Loaded config from {config_path}")
            else:
                logger.warning(f"Config file not found: {config_path}, using empty config")
        
        # Load ClinicalTrials-specific configuration
        ct_config = load_clinical_trials_config()
        
        # Get data files
        data_files = get_clinical_trials_data_files(ct_config, main_config)
        logger.info(f"Data files configuration: {data_files}")
        
        # Create adapters
        adapters = create_clinical_trials_adapters(data_files, main_config)
        
        if not adapters:
            logger.error("No ClinicalTrials adapters created. Check your configuration.")
            return False
        
        logger.info(f"Created {len(adapters)} ClinicalTrials adapters")
        
        # Initialize BioCypher
        bc = BioCypher(
            schema_config_path=schema_config_path,
            biocypher_config_path="/app/config/biocypher_config.yaml"
        )
        
        # Process each adapter
        for i, adapter in enumerate(adapters):
            logger.info(f"Processing adapter {i+1}/{len(adapters)}: {type(adapter).__name__}")
            
            try:
                # ClinicalTrials adapter does parsing in constructor, no parse_data() method needed
                logger.info("Getting nodes and edges from adapter...")
                
                # Write nodes
                logger.info("Writing nodes...")
                bc.write_nodes(adapter.get_nodes())
                
                # Write edges
                logger.info("Writing edges...")
                bc.write_edges(adapter.get_edges())
                
            except Exception as e:
                logger.error(f"Error processing adapter {type(adapter).__name__}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # Write to files
        logger.info("Writing knowledge graph to files...")
        bc.write_import_call()
        
        # Convert to Neptune format if requested
        if convert_to_neptune_format:
            logger.info("Converting to Neptune format...")
            convert_to_neptune(output_dir)
        
        # Calculate execution time
        end_time = time.time()
        execution_time = end_time - start_time
        
        logger.info("=" * 80)
        logger.info("CLINICALTRIALS KNOWLEDGE GRAPH BUILD COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Execution time: {execution_time:.2f} seconds")
        logger.info(f"Output directory: {output_dir}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error building ClinicalTrials knowledge graph: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Build ClinicalTrials Knowledge Graph')
    parser.add_argument('--config', '-c', 
                       default='/app/config/kg_config_clinicaltrials.yaml',
                       help='Path to configuration file')
    parser.add_argument('--schema', '-s',
                       default='/app/config/schema_clinical_trial.yaml', 
                       help='Path to schema configuration file')
    parser.add_argument('--output', '-o',
                       default='/app/output',
                       help='Output directory')
    
    args = parser.parse_args()
    
    # Build the knowledge graph
    success = build_clinical_trials_knowledge_graph(
        config_path=args.config,
        schema_config_path=args.schema,
        output_dir=args.output
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
