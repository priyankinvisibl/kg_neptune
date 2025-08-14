#!/usr/bin/env python3
"""
CIViC Knowledge Graph Builder

This script builds a knowledge graph from CIViC data using BioCypher.
Integrated with the multi-builder system for S3 upload and Neptune loading.
"""

import os
import sys
import time
import argparse
import yaml
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from biocypher import BioCypher
from utils.filehandler import FileHandler
from adapters.civic.civic_adapter_fixed import CivicAdapterFixed
from adapters.civic.civic_assertion_adapter import CivicAssertionAdapter
from utils.neptune_converter import convert_to_neptune

def load_config(config_path="/app/config/kg_config.yaml"):
    """
    Load complete configuration from a YAML file
    
    Args:
        config_path: Path to the config file
        
    Returns:
        Dictionary containing all configuration
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config
    
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        import traceback
        traceback.print_exc()
        return {}

def get_civic_data_files(config, main_config=None):
    """
    Get CIViC data files from URLs or use existing files
    
    Args:
        config: CIViC configuration
        main_config: Main configuration (for URL downloads)
        
    Returns:
        Dictionary of file paths
    """
    file_handler = FileHandler()
    data_files = {}
    
    # Default URLs if not in config
    default_urls = {
        'features': 'https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-FeatureSummaries.tsv',
        'variants': 'https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-VariantSummaries.tsv',
        'molecular_profiles': 'https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-MolecularProfileSummaries.tsv',
        'evidence': 'https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-ClinicalEvidenceSummaries.tsv',
        'assertions': 'https://civicdb.org/downloads/01-Jul-2025/01-Jul-2025-AssertionSummaries.tsv'
    }
    
    # Use URLs from main config if available
    if main_config and 'datasets' in main_config and 'civic' in main_config['datasets']:
        civic_datasets = main_config['datasets']['civic']
        for key, dataset_config in civic_datasets.items():
            if 'url' in dataset_config:
                default_urls[key] = dataset_config['url']
    
    # Download or locate files
    for file_type, url in default_urls.items():
        logger.info(f"Getting CIViC {file_type} data...")
        
        # Try to download from URL
        try:
            file_path = file_handler.download_file(f"civic_{file_type}", url, force=False)
            if file_path and file_path.exists():
                data_files[file_type] = str(file_path)
                logger.info(f"✅ {file_type}: {file_path}")
            else:
                logger.warning(f"⚠️  Failed to download {file_type} from {url}")
        except Exception as e:
            logger.error(f"Error downloading {file_type}: {e}")
    
    return data_files

def build_civic_knowledge_graph(data_dir=None, output_dir=None, download_data=False, 
                               convert_to_neptune_format=False, config=None):
    """
    Build CIViC knowledge graph using BioCypher
    
    Args:
        data_dir: Directory containing CIViC data files
        output_dir: Directory to output the knowledge graph
        download_data: Whether to download data from CIViC URLs
        convert_to_neptune_format: Whether to convert the output to Neptune format
        config: Configuration dictionary
    """
    start_time = time.time()
    
    # Load config if not provided
    if not config:
        config = load_config()
    
    # Check data directory
    if not data_dir:
        data_dir = os.path.join(os.getcwd(), "civic")
    
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    # Get CIViC data files
    if download_data or not any(os.path.exists(os.path.join(data_dir, f)) for f in [
        "01-Jul-2025-FeatureSummaries.tsv",
        "01-Jul-2025-VariantSummaries.tsv", 
        "01-Jul-2025-MolecularProfileSummaries.tsv",
        "01-Jul-2025-ClinicalEvidenceSummaries.tsv",
        "01-Jul-2025-AssertionSummaries.tsv"
    ]):
        logger.info("Downloading CIViC data files...")
        data_files = get_civic_data_files({}, config)
        
        # Copy downloaded files to data_dir with proper names
        file_mapping = {
            'features': '01-Jul-2025-FeatureSummaries.tsv',
            'variants': '01-Jul-2025-VariantSummaries.tsv',
            'molecular_profiles': '01-Jul-2025-MolecularProfileSummaries.tsv',
            'evidence': '01-Jul-2025-ClinicalEvidenceSummaries.tsv',
            'assertions': '01-Jul-2025-AssertionSummaries.tsv'
        }
        
        for file_type, target_name in file_mapping.items():
            if file_type in data_files:
                source_path = data_files[file_type]
                target_path = os.path.join(data_dir, target_name)
                if source_path != target_path:
                    import shutil
                    shutil.copy2(source_path, target_path)
                    logger.info(f"Copied {source_path} -> {target_path}")
    
    # Initialize BioCypher
    logger.info("Initializing BioCypher...")
    schema_file = os.path.join("config", "schema_civic.yaml")
    bc = BioCypher(
        schema_config_path=schema_file,
        output_directory=output_dir
    )
    
    # Initialize CIViC adapter
    logger.info("Initializing CIViC adapter...")
    civic_adapter = CivicAdapterFixed(data_dir=data_dir)
    
    logger.info(f"Adapter initialization took: {time.time() - start_time:.2f} seconds")
    
    # Parse CIViC data
    parse_start = time.time()
    civic_adapter.parse_data()
    logger.info(f"Data parsing took: {time.time() - parse_start:.2f} seconds")
    
    # Write nodes
    logger.info("Writing nodes to BioCypher...")
    nodes_start = time.time()
    
    # Get all nodes from the fixed adapter
    all_nodes = list(civic_adapter.get_nodes())
    logger.info(f"Total nodes extracted: {len(all_nodes):,}")
    
    # Write nodes to BioCypher
    bc.write_nodes(all_nodes)
    logger.info("Nodes written successfully")
    
    logger.info(f"Node writing took: {time.time() - nodes_start:.2f} seconds")
    
    # Write edges
    logger.info("Writing edges to BioCypher...")
    edges_start = time.time()
    
    # Get all edges from the fixed adapter
    all_edges = list(civic_adapter.get_edges())
    logger.info(f"Total edges extracted: {len(all_edges):,}")
    
    # Write edges to BioCypher
    bc.write_edges(all_edges)
    logger.info("Edges written successfully")
    
    logger.info(f"Edge writing took: {time.time() - edges_start:.2f} seconds")
    
    # Complete the BioCypher process
    logger.info("Completing BioCypher process...")
    bc.write_import_call()
    logger.info("Import call file written successfully")
    
    # Get the output directory
    output_base = Path(output_dir) if output_dir else Path("biocypher-out")
    
    # Find the latest output directory
    latest_dir = None
    if output_base.exists():
        subdirs = [d for d in output_base.iterdir() if d.is_dir()]
        if subdirs:
            latest_dir = max(subdirs, key=lambda x: x.stat().st_mtime)
    
    # Convert to Neptune format if requested
    if convert_to_neptune_format and latest_dir:
        logger.info("\nConverting to Neptune format...")
        neptune_dir = latest_dir.parent / f"{latest_dir.name}_neptune"
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), schema_file)
        neptune_output = convert_to_neptune(str(latest_dir), str(neptune_dir), schema_file=schema_path)
        logger.info(f"Neptune conversion complete. Files available in: {neptune_output}")
    
    logger.info(f"CIViC knowledge graph build complete! Total time: {time.time() - start_time:.2f} seconds")
    
    return output_base

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Build CIViC Knowledge Graph")
    parser.add_argument("--data-dir", "-d", help="Directory containing CIViC data files")
    parser.add_argument("--output-dir", "-o", help="Output directory for the knowledge graph")
    parser.add_argument("--download", "-w", action="store_true", help="Download data from CIViC URLs")
    parser.add_argument("--neptune", "-n", action="store_true", help="Convert output to Neptune format")
    parser.add_argument("--config", "-c", help="Configuration file path")
    args = parser.parse_args()
    
    # Load config if provided
    config = None
    if args.config:
        config = load_config(args.config)
    
    try:
        output_dir = build_civic_knowledge_graph(
            data_dir=args.data_dir,
            output_dir=args.output_dir,
            download_data=args.download,
            convert_to_neptune_format=args.neptune,
            config=config
        )
        
        logger.info("\n" + "=" * 60)
        logger.info("CIViC Knowledge Graph Built Successfully!")
        logger.info("=" * 60)
        if output_dir:
            logger.info(f"Output available in: {output_dir}")
        
    except Exception as e:
        logger.error(f"Error building knowledge graph: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
