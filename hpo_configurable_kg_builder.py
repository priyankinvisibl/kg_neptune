#!/usr/bin/env python3
"""
HPO (Human Phenotype Ontology) Knowledge Graph Builder
Builds knowledge graphs from HPO data files with configurable properties
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
from adapters.hpo.phenotype_hpoa_adapter import PhenotypeHpoaAdapter
from adapters.hpo.phenotype_to_genes_adapter import PhenotypeToGenesAdapter
from adapters.hpo.genes_to_disease_adapter import GenesToDiseaseAdapter
from utils.neptune_converter import convert_to_neptune

def load_hpo_config(config_path="/app/config/hpo_column_config.yaml"):
    """
    Load HPO configuration from a YAML file
    
    Args:
        config_path: Path to the HPO config file
        
    Returns:
        Dictionary containing HPO configuration
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config
    
    except Exception as e:
        logger.error(f"Error loading HPO config: {e}")
        import traceback
        traceback.print_exc()
        return {}

def get_hpo_data_files(config, main_config=None):
    """
    Get HPO data file paths from configuration, with support for URL downloads
    
    Args:
        config: HPO configuration dictionary
        main_config: Main configuration dictionary (for URL-based downloads)
        
    Returns:
        Dictionary with file paths or URLs
    """
    files = {}
    
    # If main_config is provided and has HPO URLs, use those
    if main_config and 'datasets' in main_config and 'hpo' in main_config['datasets']:
        hpo_datasets = main_config['datasets']['hpo']
        
        for dataset_name, dataset_config in hpo_datasets.items():
            if 'url' in dataset_config:
                files[dataset_name] = {
                    'url': dataset_config['url'],
                    'adapter': dataset_config.get('adapter', ''),
                    'description': dataset_config.get('description', '')
                }
        
        logger.info(f"Using URL-based HPO configuration: {files}")
        return files
    
    # Fallback to file-based configuration
    if 'phenotype_hpoa' in config:
        files['phenotype_hpoa'] = config['phenotype_hpoa'].get('file_path', 'hpo/phenotype.hpoa')
    
    if 'phenotype_to_genes' in config:
        files['phenotype_to_genes'] = config['phenotype_to_genes'].get('file_path', 'hpo/phenotype_to_genes.txt')
    
    if 'genes_to_disease' in config:
        files['genes_to_disease'] = config['genes_to_disease'].get('file_path', 'hpo/genes_to_disease.txt')
    
    logger.info(f"Using file-based HPO configuration: {files}")
    return files

def download_hpo_file(url, local_path):
    """
    Download HPO data file from URL
    
    Args:
        url: URL to download from
        local_path: Local path to save the file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"Downloading HPO file from {url}")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Download file
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Successfully downloaded {url} -> {local_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to download {url}: {e}")
        return False

def create_adapters(data_files, use_urls=False):
    """
    Create HPO adapters for each data file, with support for URL downloads
    
    Args:
        data_files: Dictionary with file paths or URL configurations
        use_urls: Boolean indicating if data_files contains URL configurations
        
    Returns:
        List of adapter instances
    """
    adapters = []
    
    if use_urls:
        # Handle URL-based configuration
        for dataset_name, dataset_config in data_files.items():
            if 'url' in dataset_config:
                url = dataset_config['url']
                local_path = f"/tmp/{dataset_name}.txt"
                
                # Download file
                if download_hpo_file(url, local_path):
                    # Create appropriate adapter based on dataset name
                    if dataset_name == 'phenotype_hpoa':
                        logger.info(f"Creating PhenotypeHpoaAdapter for {local_path}")
                        adapters.append(PhenotypeHpoaAdapter(local_path))
                    elif dataset_name == 'phenotype_to_genes':
                        logger.info(f"Creating PhenotypeToGenesAdapter for {local_path}")
                        adapters.append(PhenotypeToGenesAdapter(local_path))
                    elif dataset_name == 'genes_to_disease':
                        logger.info(f"Creating GenesToDiseaseAdapter for {local_path}")
                        adapters.append(GenesToDiseaseAdapter(local_path))
                    else:
                        logger.warning(f"Unknown dataset type: {dataset_name}")
                else:
                    logger.error(f"Failed to download {dataset_name} from {url}")
    else:
        # Handle file-based configuration (original logic)
        if 'phenotype_hpoa' in data_files:
            file_path = data_files['phenotype_hpoa']
            if os.path.exists(file_path):
                logger.info(f"Creating PhenotypeHpoaAdapter for {file_path}")
                adapters.append(PhenotypeHpoaAdapter(file_path))
            else:
                logger.warning(f"HPO phenotype.hpoa file not found: {file_path}")
        
        if 'phenotype_to_genes' in data_files:
            file_path = data_files['phenotype_to_genes']
            if os.path.exists(file_path):
                logger.info(f"Creating PhenotypeToGenesAdapter for {file_path}")
                adapters.append(PhenotypeToGenesAdapter(file_path))
            else:
                logger.warning(f"HPO phenotype_to_genes.txt file not found: {file_path}")
        
        if 'genes_to_disease' in data_files:
            file_path = data_files['genes_to_disease']
            if os.path.exists(file_path):
                logger.info(f"Creating GenesToDiseaseAdapter for {file_path}")
                adapters.append(GenesToDiseaseAdapter(file_path))
            else:
                logger.warning(f"HPO genes_to_disease.txt file not found: {file_path}")
    
    return adapters

def build_hpo_knowledge_graph(config_path="/app/config/hpo_column_config.yaml", 
                              output_dir="/app/output/hpo",
                              convert_to_neptune_format=False,
                              main_config=None):
    """
    Build HPO knowledge graph from configured data files
    
    Args:
        config_path: Path to HPO configuration file
        output_dir: Output directory for BioCypher files
        convert_to_neptune_format: Whether to convert to Neptune format
        main_config: Main configuration dictionary (for URL-based downloads)
        
    Returns:
        Dictionary with build results
    """
    
    logger.info("=" * 60)
    logger.info("HPO KNOWLEDGE GRAPH BUILDER")
    logger.info("=" * 60)
    
    start_time = time.time()
    
    try:
        # Load HPO configuration
        logger.info(f"Loading HPO configuration from {config_path}")
        hpo_config = load_hpo_config(config_path)
        
        if not hpo_config:
            logger.error("Failed to load HPO configuration")
            return {"status": "failed", "error": "Configuration loading failed"}
        
        # Get data file paths or URLs
        data_files = get_hpo_data_files(hpo_config, main_config)
        logger.info(f"HPO data configuration: {data_files}")
        
        # Determine if we're using URLs or files
        use_urls = main_config and 'datasets' in main_config and 'hpo' in main_config['datasets']
        logger.info(f"Using URL-based downloads: {use_urls}")
        
        # Create adapters
        adapters = create_adapters(data_files, use_urls)
        
        if not adapters:
            logger.warning("No valid HPO adapters created - no data files found")
            return {"status": "failed", "error": "No valid data files found"}
        
        logger.info(f"Created {len(adapters)} HPO adapters")
        
        # Print adapter statistics
        for adapter in adapters:
            stats = adapter.get_statistics()
            logger.info(f"{adapter.__class__.__name__}: {stats}")
        
        # Setup BioCypher
        logger.info("Setting up BioCypher...")
        
        # Use HPO schema configuration
        schema_config_path = "/app/config/schema_config_hpo.yaml"
        if not os.path.exists(schema_config_path):
            schema_config_path = "config/schema_config_hpo.yaml"
        
        # Use general biocypher configuration
        biocypher_config_path = "/app/config/biocypher_config.yaml"
        if not os.path.exists(biocypher_config_path):
            biocypher_config_path = "config/biocypher_config.yaml"
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize BioCypher with proper configuration
        bc = BioCypher(
            schema_config_path=schema_config_path,
            biocypher_config_path=biocypher_config_path
        )
        
        # Set output directory manually
        bc.output_dir = output_dir
        
        # Process nodes from all adapters
        logger.info("Processing nodes...")
        all_nodes = []
        
        for adapter in adapters:
            logger.info(f"Processing nodes from {adapter.__class__.__name__}")
            adapter_nodes = list(adapter.get_nodes())
            all_nodes.extend(adapter_nodes)
            logger.info(f"Added {len(adapter_nodes)} nodes from {adapter.__class__.__name__}")
        
        logger.info(f"Total nodes collected: {len(all_nodes)}")
        
        # Process edges from all adapters
        logger.info("Processing edges...")
        all_edges = []
        
        for adapter in adapters:
            logger.info(f"Processing edges from {adapter.__class__.__name__}")
            adapter_edges = list(adapter.get_edges())
            all_edges.extend(adapter_edges)
            logger.info(f"Added {len(adapter_edges)} edges from {adapter.__class__.__name__}")
        
        logger.info(f"Total edges collected: {len(all_edges)}")
        
        # Write knowledge graph
        logger.info("Writing knowledge graph...")
        
        # Write nodes
        try:
            bc.write_nodes(all_nodes)
            logger.info(f"Successfully wrote {len(all_nodes)} nodes")
        except Exception as e:
            logger.error(f"Error writing nodes: {e}")
            import traceback
            traceback.print_exc()
        
        # Write edges
        try:
            bc.write_edges(all_edges)
            logger.info(f"Successfully wrote {len(all_edges)} edges")
        except Exception as e:
            logger.error(f"Error writing edges: {e}")
            import traceback
            traceback.print_exc()
        
        # Get output summary
        summary = bc.summary()
        logger.info("BioCypher Summary:")
        logger.info(summary)
        
        end_time = time.time()
        duration = end_time - start_time
        
        result = {
            "status": "success",
            "duration": duration,
            "output_dir": output_dir,
            "nodes": len(all_nodes),
            "edges": len(all_edges),
            "adapters_used": len(adapters),
            "summary": summary
        }
        
        # Convert to Neptune format if requested
        if convert_to_neptune_format:
            logger.info("Converting to Neptune format...")
            neptune_dir = f"{output_dir}_neptune"
            neptune_result = convert_to_neptune(output_dir, neptune_dir)
            result["neptune_dir"] = neptune_result
        
        logger.info(f"HPO Knowledge Graph build completed in {duration:.2f} seconds")
        logger.info(f"Output directory: {output_dir}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error building HPO knowledge graph: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "status": "failed",
            "error": str(e),
            "output_dir": output_dir
        }

def main():
    """Main function for command line usage"""
    parser = argparse.ArgumentParser(description='Build HPO Knowledge Graph')
    parser.add_argument('--config', default='/app/config/hpo_column_config.yaml',
                        help='Path to HPO configuration file')
    parser.add_argument('--output-dir', default='/app/output/hpo',
                        help='Output directory for knowledge graph files')
    parser.add_argument('--convert-to-neptune', action='store_true',
                        help='Convert output to Neptune format')
    
    args = parser.parse_args()
    
    # Build knowledge graph
    result = build_hpo_knowledge_graph(
        config_path=args.config,
        output_dir=args.output_dir,
        convert_to_neptune_format=args.convert_to_neptune
    )
    
    if result["status"] == "success":
        print("✅ HPO Knowledge Graph build completed successfully!")
        print(f"Duration: {result['duration']:.2f} seconds")
        print(f"Nodes: {result['nodes']}")
        print(f"Edges: {result['edges']}")
        print(f"Output: {result['output_dir']}")
    else:
        print("❌ HPO Knowledge Graph build failed!")
        print(f"Error: {result['error']}")
        sys.exit(1)

if __name__ == "__main__":
    main()
