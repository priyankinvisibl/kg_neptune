#!/usr/bin/env python3
"""
CIViC Knowledge Graph Builder

This script builds a knowledge graph from CIViC data using BioCypher.
"""

import os
import sys
import time
import argparse
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from biocypher import BioCypher
from adapters.civic.civic_adapter import CivicAdapter
from adapters.civic.civic_assertion_adapter import CivicAssertionAdapter
from utils.neptune_converter import convert_to_neptune

def write_assertions_directly(output_dir, civic_adapter):
    """Write CIViC assertions directly to a CSV file in BioCypher format."""
    logger.info("Writing assertions directly to output file...")
    
    # Path to the BioCypher Assertion-part000.csv file
    assertion_file = os.path.join(output_dir, "Assertion-part000.csv")
    
    # Write assertions directly to the CSV file
    with open(assertion_file, 'w') as f:
        for assertion_id, assertion in sorted(civic_adapter.assertions.items()):
            # Format the assertion data according to BioCypher's expected format
            description = assertion.get('description', '').replace('"', '\\"')  # Escape double quotes
            assertion_type = assertion.get('assertion_type', '')
            assertion_direction = assertion.get('assertion_direction', '')
            clinical_significance = assertion.get('clinical_significance', '')
            disease = assertion.get('disease', '')
            doid = assertion.get('doid', '')
            therapies = assertion.get('therapies', '')
            summary = assertion.get('summary', '').replace('"', '\\"')  # Escape double quotes
            amp_category = assertion.get('amp_category', '')
            data_source = assertion.get('data_source', '')
            
            # Format the line according to BioCypher's format
            line = f'{assertion_id}\t"{description}"\t"{assertion_type}"\t"{assertion_direction}"\t"{clinical_significance}"\t"{disease}"\t"{doid}"\t"{therapies}"\t"{summary}"\t"{amp_category}"\t"{data_source}"\t"id"\t"Assertion|Biolink:Association"\n'
            f.write(line)
    
    logger.info(f"Successfully wrote {len(civic_adapter.assertions)} assertions to {assertion_file}")

def build_civic_knowledge_graph(data_dir=None, output_dir=None, download_data=False, convert_to_neptune_format=False):
    """
    Build CIViC knowledge graph using BioCypher
    
    Args:
        data_dir: Directory containing CIViC data files
        output_dir: Directory to output the knowledge graph
        download_data: Whether to download data from CIViC API
        convert_to_neptune_format: Whether to convert the output to Neptune format
    """
    start_time = time.time()
    
    # Check data directory
    if not data_dir:
        data_dir = os.path.join(os.getcwd(), "civic")
    
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    # Initialize BioCypher
    logger.info("Initializing BioCypher...")
    schema_file = os.path.join("config", "schema_config_civic.yaml")
    bc = BioCypher(
        schema_config_path=schema_file,
        output_directory=output_dir
    )
    
    # Initialize CIViC adapter
    logger.info("Initializing CIViC adapter...")
    civic_adapter = CivicAdapter(data_dir=data_dir)
    
    # Initialize CIViC assertion adapter
    logger.info("Initializing CIViC assertion adapter...")
    assertion_adapter = CivicAssertionAdapter(data_dir=data_dir)
    
    logger.info(f"Adapter initialization took: {time.time() - start_time:.2f} seconds")
    
    # Download data if requested
    if download_data:
        logger.info("Downloading CIViC data...")
        civic_adapter.download_data(force=True)
    
    # Parse CIViC data
    parse_start = time.time()
    civic_adapter.parse_data()
    assertion_adapter.parse_data()
    logger.info(f"Data parsing took: {time.time() - parse_start:.2f} seconds")
    
    # Write nodes
    logger.info("Writing nodes to BioCypher...")
    nodes_start = time.time()
    
    # Get all nodes except assertions
    all_nodes = []
    
    # Get nodes from the main adapter (excluding assertions)
    for node in civic_adapter.get_nodes():
        if node[1] != 'assertion':  # Skip assertions from the main adapter
            all_nodes.append(node)
    
    # Get assertion nodes from the assertion adapter
    assertion_nodes = list(assertion_adapter.get_nodes())
    all_nodes.extend(assertion_nodes)
    
    logger.info(f"Total nodes extracted: {len(all_nodes):,}")
    
    # Write nodes to BioCypher
    bc.write_nodes(all_nodes)
    logger.info("Nodes written successfully")
    
    logger.info(f"Node writing took: {time.time() - nodes_start:.2f} seconds")
    
    # Write edges
    logger.info("Writing edges to BioCypher...")
    edges_start = time.time()
    
    # Get all edges
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
    output_base = output_dir if output_dir else Path("biocypher-out")
    
    # Find the latest output directory
    latest_dir = None
    if output_base.exists():
        subdirs = [d for d in output_base.iterdir() if d.is_dir()]
        if subdirs:
            latest_dir = max(subdirs, key=lambda x: x.stat().st_mtime)
    
    # Write assertions directly to the output file
    if latest_dir:
        write_assertions_directly(latest_dir, civic_adapter)
    
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
    parser.add_argument("--download", "-w", action="store_true", help="Download data from CIViC API")
    parser.add_argument("--neptune", "-n", action="store_true", help="Convert output to Neptune format")
    args = parser.parse_args()
    
    try:
        output_dir = build_civic_knowledge_graph(
            data_dir=args.data_dir,
            output_dir=args.output_dir,
            download_data=args.download,
            convert_to_neptune_format=args.neptune
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
