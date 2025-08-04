"""
DGIdb Knowledge Graph Builder
Follows the same pattern as CIViC and other builders
"""

import os
import time
import logging
from pathlib import Path

from biocypher import BioCypher
from utils.filehandler import FileHandler
from adapters.dgidb.dgidb_adapter import DgidbAdapter
from utils.neptune_converter import convert_to_neptune

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def build_dgidb_knowledge_graph(data_dir=None, output_dir=None, download_data=False, 
                               convert_to_neptune_format=False, config=None):
    """
    Build DGIdb knowledge graph using BioCypher
    
    Args:
        data_dir: Directory containing DGIdb data files
        output_dir: Output directory for BioCypher files
        download_data: Whether to download data from URLs
        convert_to_neptune_format: Whether to convert to Neptune format
        config: Configuration dictionary containing URLs and settings
    
    Returns:
        dict: Build results and metadata
    """
    
    start_time = time.time()
    logger.info("============================================================")
    logger.info("Building DGIdb Knowledge Graph")
    logger.info("============================================================")
    
    # Set default directories
    if not data_dir:
        data_dir = "/app/data"
    
    # Ensure data directory exists
    os.makedirs(data_dir, exist_ok=True)
    
    # Initialize DGIdb adapter
    logger.info("Initializing DGIdb adapter...")
    dgidb_adapter = DgidbAdapter(data_dir=data_dir)
    
    # Download data if requested
    if download_data and config:
        logger.info("Downloading DGIdb data files...")
        dgidb_adapter.download_data(config)
        
        # Copy files to expected locations (similar to CIViC pattern)
        dgidb_dir = "/app/dgidb"
        os.makedirs(dgidb_dir, exist_ok=True)
        
        import shutil
        files_to_copy = [
            ("dgidb_interactions.tsv", "interactions.tsv"),
            ("dgidb_genes.tsv", "genes.tsv"),
            ("dgidb_drugs.tsv", "drugs.tsv"),
            ("dgidb_categories.tsv", "categories.tsv")
        ]
        
        for src_name, dst_name in files_to_copy:
            src_path = os.path.join(data_dir, src_name)
            dst_path = os.path.join(dgidb_dir, dst_name)
            if os.path.exists(src_path):
                shutil.copy2(src_path, dst_path)
                logger.info(f"Copied {src_path} -> {dst_path}")
    
    logger.info(f"Adapter initialization took: {time.time() - start_time:.2f} seconds")
    
    # Parse DGIdb data
    parse_start = time.time()
    dgidb_adapter.parse_data()
    logger.info(f"Data parsing took: {time.time() - parse_start:.2f} seconds")
    
    # Initialize BioCypher
    logger.info("Initializing BioCypher...")
    schema_file = os.path.join("config", "schema_dgidb.yaml")
    bc = BioCypher(
        schema_config_path=schema_file,
        output_directory=output_dir
    )
    
    # Write nodes
    logger.info("Writing nodes to BioCypher...")
    nodes_start = time.time()
    
    # Get all nodes from the adapter
    all_nodes = list(dgidb_adapter.get_nodes())
    logger.info(f"Total nodes extracted: {len(all_nodes):,}")
    
    # Write nodes to BioCypher
    bc.write_nodes(all_nodes)
    logger.info("Nodes written successfully")
    
    logger.info(f"Node writing took: {time.time() - nodes_start:.2f} seconds")
    
    # Write edges
    logger.info("Writing edges to BioCypher...")
    edges_start = time.time()
    
    # Get all edges from the adapter
    all_edges = list(dgidb_adapter.get_edges())
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
    
    total_time = time.time() - start_time
    logger.info(f"DGIdb knowledge graph build complete! Total time: {total_time:.2f} seconds")
    
    # Convert to Neptune format if requested
    if convert_to_neptune_format and latest_dir:
        logger.info("Converting to Neptune format...")
        neptune_dir = convert_to_neptune(
            str(latest_dir),
            output_dir=f"{latest_dir}_neptune"
        )
        logger.info(f"Neptune format created: {neptune_dir}")
    
    return {
        "status": "success",
        "output_dir": str(latest_dir) if latest_dir else None,
        "total_time": total_time,
        "nodes_count": len(all_nodes),
        "edges_count": len(all_edges)
    }

if __name__ == "__main__":
    # Test the builder
    result = build_dgidb_knowledge_graph(
        download_data=True,
        convert_to_neptune_format=False
    )
    print(f"Build result: {result}")
