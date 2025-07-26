#!/usr/bin/env python3
"""
MESH XML data processing - processes desc2025.xml file
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
from adapters.mesh.mesh_xml_adapter import MeshXmlAdapter
from utils.neptune_converter import convert_to_neptune

def build_mesh_xml_knowledge_graph(input_file=None, output_dir=None, convert_to_neptune_format=False):
    """
    Build MESH knowledge graph from XML file using BioCypher
    
    Args:
        input_file: Path to the MESH XML file
        output_dir: Directory to output the knowledge graph
        convert_to_neptune_format: Whether to convert the output to Neptune format
    """
    start_time = time.time()
    
    # Check input file
    if not input_file:
        input_file = "desc2025.xml"
    
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return None
    
    # Initialize BioCypher with XML-specific config
    schema_file = os.path.join("config", "schema_config_xml.yaml")
    bc = BioCypher(
        schema_config_path=schema_file,
        output_directory=output_dir
    )
    
    # Use the XML adapter
    adapter = MeshXmlAdapter(input_file)
    
    logger.info(f"Adapter initialization took: {time.time() - start_time:.2f} seconds")
    
    # Get statistics first
    stats = adapter.get_statistics()
    logger.info("=== MESH XML Data Statistics ===")
    for key, value in stats.items():
        logger.info(f"{key}: {value:,}")
    
    # Process all nodes
    logger.info("Processing all node types...")
    nodes_start = time.time()
    
    all_nodes = list(adapter.get_nodes())
    logger.info(f"Node extraction took: {time.time() - nodes_start:.2f} seconds")
    logger.info(f"Total nodes extracted: {len(all_nodes):,}")
    
    # Process all edges
    logger.info("Processing all relationship types...")
    edges_start = time.time()
    
    all_edges = list(adapter.get_edges())
    logger.info(f"Edge extraction took: {time.time() - edges_start:.2f} seconds")
    logger.info(f"Total edges extracted: {len(all_edges):,}")
    
    # Write to BioCypher
    logger.info("Writing to BioCypher output...")
    write_start = time.time()
    
    bc.write_nodes(all_nodes)
    bc.write_edges(all_edges)
    bc.write_import_call()
    
    logger.info(f"Writing took: {time.time() - write_start:.2f} seconds")
    
    # Get the output directory
    output_base = output_dir if output_dir else Path("biocypher-out")
    
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
    
    total_time = time.time() - start_time
    logger.info(f"Total execution time: {total_time:.2f} seconds")
    
    # Show comprehensive summary
    logger.info("=== XML Processing Summary ===")
    logger.info(f"Descriptors: {stats['descriptors']:,}")
    logger.info(f"Concepts: {stats['concepts']:,}")
    logger.info(f"Terms: {stats['terms']:,}")
    logger.info(f"Qualifiers: {stats['qualifiers']:,}")
    logger.info(f"Total entities: {stats['total_entities']:,}")
    logger.info(f"Relationships: {stats['relationships']:,}")
    
    # Show node type breakdown
    node_types = {}
    for node_id, node_type, properties in all_nodes:
        node_types[node_type] = node_types.get(node_type, 0) + 1
    
    logger.info("\n=== Node Type Breakdown ===")
    for node_type, count in sorted(node_types.items()):
        logger.info(f"{node_type}: {count:,}")
    
    # Show edge type breakdown
    edge_types = {}
    for source, target, edge_type, properties in all_edges:
        edge_types[edge_type] = edge_types.get(edge_type, 0) + 1
    
    logger.info("\n=== Edge Type Breakdown ===")
    for edge_type, count in sorted(edge_types.items()):
        logger.info(f"{edge_type}: {count:,}")
    
    # Show sample data
    logger.info("\n=== Sample Descriptor ===")
    if all_nodes:
        sample_node = all_nodes[0]
        logger.info(f"ID: {sample_node[0]}")
        logger.info(f"Type: {sample_node[1]}")
        logger.info("Properties:")
        for key, value in sample_node[2].items():
            if isinstance(value, str) and len(value) > 100:
                logger.info(f"  {key}: {value[:100]}...")
            else:
                logger.info(f"  {key}: {value}")
    
    logger.info("\n=== Sample Relationship ===")
    if all_edges:
        sample_edge = all_edges[0]
        logger.info(f"Source: {sample_edge[0]}")
        logger.info(f"Target: {sample_edge[1]}")
        logger.info(f"Type: {sample_edge[2]}")
        logger.info(f"Properties: {sample_edge[3]}")
    
    return output_base

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Build MESH Knowledge Graph from XML file")
    parser.add_argument("--input-file", "-i", help="Path to the MESH XML file")
    parser.add_argument("--output-dir", "-o", help="Output directory for the knowledge graph")
    parser.add_argument("--neptune", "-n", action="store_true", help="Convert output to Neptune format")
    args = parser.parse_args()
    
    try:
        output_dir = build_mesh_xml_knowledge_graph(
            input_file=args.input_file,
            output_dir=args.output_dir,
            convert_to_neptune_format=args.neptune
        )
        
        logger.info("\n" + "=" * 60)
        logger.info("MESH XML Knowledge Graph Built Successfully!")
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
