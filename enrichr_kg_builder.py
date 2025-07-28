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
from adapters.enrichr.reactome_adapter import ReactomeAdapter
from adapters.enrichr.wikipathway_adapter import WikiPathwayAdapter
from adapters.enrichr.biological_process_adapter import BiologicalProcessAdapter
from adapters.enrichr.molecular_function_adapter import MolecularFunctionAdapter
from adapters.enrichr.cell_component_adapter import CellComponentAdapter
from adapters.enrichr.drugdb_adapter import DrugDBAdapter
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

def load_datasets_config(config):
    """
    Load datasets configuration from the config dictionary
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Dictionary of datasets for the specified builder
    """
    try:
        # Get the datasets for enrichr (can be extended for other builders)
        datasets_config = config.get('datasets', {}).get('enrichr', {})
        
        # Map adapter names to actual adapter classes
        adapter_mapping = {
            'ReactomeAdapter': ReactomeAdapter,
            'WikiPathwayAdapter': WikiPathwayAdapter,
            'BiologicalProcessAdapter': BiologicalProcessAdapter,
            'MolecularFunctionAdapter': MolecularFunctionAdapter,
            'CellComponentAdapter': CellComponentAdapter,
            'DrugDBAdapter': DrugDBAdapter
        }
        
        # Replace adapter names with actual adapter classes
        for dataset_name, dataset_info in datasets_config.items():
            adapter_name = dataset_info.get('adapter')
            if adapter_name in adapter_mapping:
                dataset_info['adapter'] = adapter_mapping[adapter_name]
            else:
                logger.warning(f"Unknown adapter: {adapter_name} for dataset {dataset_name}")
                dataset_info['adapter'] = None
        
        return datasets_config
    
    except Exception as e:
        logger.error(f"Error loading datasets config: {e}")
        import traceback
        traceback.print_exc()
        return {}

def build_enrichr_knowledge_graph_from_config(config_path="/app/config/kg_config.yaml"):
    """
    Build Enrichr knowledge graph using configuration from YAML file
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Path to the output directory
    """
    # Load configuration
    config = load_config(config_path)
    if not config:
        logger.error("Failed to load configuration")
        return None
    
    # Extract configuration values
    general_config = config.get('general', {})
    s3_config = config.get('s3', {})
    neptune_config = config.get('neptune', {})
    
    builders = general_config.get('builders', ['enrichr'])
    output_dir = general_config.get('output_dir', '/app/output')
    convert_to_neptune_format = general_config.get('convert_to_neptune', False)
    
    upload_to_s3 = s3_config.get('upload', False)
    s3_bucket = s3_config.get('bucket', '')
    s3_prefix = s3_config.get('prefix', '')
    
    load_to_neptune = neptune_config.get('load', False)
    neptune_endpoint = neptune_config.get('endpoint', '')
    iam_role_arn = neptune_config.get('iam_role_arn', '')
    
    logger.info("Configuration loaded:")
    logger.info(f"  Builders: {builders}")
    logger.info(f"  Output directory: {output_dir}")
    logger.info(f"  Convert to Neptune: {convert_to_neptune_format}")
    logger.info(f"  Upload to S3: {upload_to_s3}")
    logger.info(f"  Load to Neptune: {load_to_neptune}")
    
    # Call the original build function
    return build_enrichr_knowledge_graph(
        output_dir=output_dir,
        datasets=None,  # Will use all datasets from config
        convert_to_neptune_format=convert_to_neptune_format,
        upload_to_s3=upload_to_s3,
        s3_bucket=s3_bucket if s3_bucket else None,
        s3_prefix=s3_prefix if s3_prefix else None,
        load_to_neptune=load_to_neptune,
        neptune_endpoint=neptune_endpoint if neptune_endpoint else None,
        iam_role_arn=iam_role_arn if iam_role_arn else None,
        config=config
    )

def build_enrichr_knowledge_graph(output_dir=None, datasets=None, convert_to_neptune_format=False, 
                                 upload_to_s3=False, s3_bucket=None, s3_prefix=None, 
                                 load_to_neptune=False, neptune_endpoint=None, iam_role_arn=None, 
                                 config_path=None, config=None):
    """
    Build Enrichr knowledge graph using BioCypher
    
    Args:
        output_dir: Directory to output the knowledge graph
        datasets: List of datasets to include (default: all)
        convert_to_neptune_format: Whether to convert the output to Neptune format
        upload_to_s3: Whether to upload the output to S3
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix for uploaded files
        load_to_neptune: Whether to load the data into Neptune
        neptune_endpoint: Neptune endpoint URL
        iam_role_arn: IAM role ARN for Neptune to access S3
        config_path: Path to the datasets config file
        
    Returns:
        Path to the output directory
    """
    # Create output directory if specified
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        
        # Create a subdirectory for Neptune output if converting to Neptune format
        if convert_to_neptune_format:
            neptune_dir = os.path.join(output_dir, "neptune")
            os.makedirs(neptune_dir, exist_ok=True)
            
        # Create a file to indicate the output directory is ready
        with open(os.path.join(output_dir, "info.txt"), "w") as f:
            f.write("Enrichr Knowledge Graph\n")
            f.write(f"Created: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Neptune conversion: {convert_to_neptune_format}\n")
            f.write(f"S3 upload: {upload_to_s3}\n")
            f.write(f"Neptune loading: {load_to_neptune}\n")
    
    # Load datasets from config
    if config is None:
        config = load_config()
    
    all_datasets = load_datasets_config(config)
    
    # Filter datasets if specified
    if datasets:
        selected_datasets = {k: v for k, v in all_datasets.items() if k in datasets}
    else:
        selected_datasets = all_datasets
    
    # Initialize BioCypher with schema
    schema_file = os.path.join("config", "schema_enrichr.yaml")
    bc = BioCypher(
        schema_config_path=schema_file,
        biocypher_config_path=os.path.join("config", "biocypher_config.yaml")
    )
    
    # Initialize file handler
    file_handler = FileHandler(bc)
    
    logger.info("=" * 60)
    logger.info(f"Building Enrichr Knowledge Graph")
    logger.info(f"Selected datasets: {', '.join(selected_datasets.keys())}")
    logger.info("=" * 60)
    
    # Process each dataset
    all_nodes = []
    all_edges = []
    
    for dataset_name, dataset_info in selected_datasets.items():
        logger.info(f"\nProcessing {dataset_name}...")
        
        # Download dataset
        file_path = file_handler.download_file(dataset_name, dataset_info["url"])
        if not file_path:
            logger.warning(f"Skipping {dataset_name} due to download error")
            continue
        
        # Initialize adapter
        adapter = dataset_info["adapter"](file_path=file_path)
        
        # Get nodes and edges
        logger.info(f"Extracting nodes for {dataset_name}...")
        nodes_start = time.time()
        dataset_nodes = list(adapter.get_nodes())
        logger.info(f"Node extraction took: {time.time() - nodes_start:.2f} seconds")
        logger.info(f"Total nodes extracted: {len(dataset_nodes):,}")
        all_nodes.extend(dataset_nodes)
        
        logger.info(f"Extracting edges for {dataset_name}...")
        edges_start = time.time()
        dataset_edges = list(adapter.get_edges())
        logger.info(f"Edge extraction took: {time.time() - edges_start:.2f} seconds")
        logger.info(f"Total edges extracted: {len(dataset_edges):,}")
        all_edges.extend(dataset_edges)
    
    # Write knowledge graph
    logger.info("\nWriting knowledge graph...")
    write_start = time.time()
    
    # Write nodes
    try:
        bc.write_nodes(all_nodes)
        logger.info(f"Successfully wrote {len(all_nodes)} nodes")
    except Exception as e:
        logger.error(f"Error writing nodes: {e}")
        import traceback
        traceback.print_exc()
    
    # Write edges in one go
    try:
        bc.write_edges(all_edges)
        logger.info(f"Successfully wrote {len(all_edges)} edges")
    except Exception as e:
        logger.error(f"Error writing edges: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        bc.write_import_call()
    except Exception as e:
        logger.error(f"Error writing import call: {e}")
    
    logger.info(f"Writing took: {time.time() - write_start:.2f} seconds")
    
    # Get the output directory
    output_base = output_dir if output_dir else "biocypher-out"
    output_path = Path(output_base)
    
    # Find the latest output directory
    latest_dir = None
    if output_path.exists():
        subdirs = [d for d in output_path.iterdir() if d.is_dir()]
        if subdirs:
            latest_dir = max(subdirs, key=lambda x: x.stat().st_mtime)
    
    # Convert to Neptune format if requested
    neptune_output_dir = None
    if convert_to_neptune_format and latest_dir:
        logger.info("\nConverting to Neptune format...")
        
        # Create Neptune output directory in the same way BioCypher creates its output
        # Use biocypher-out directory structure which works with volume mounting
        neptune_dir = os.path.join("biocypher-out", f"{latest_dir.name}_neptune")
        
        # Ensure directory exists with proper permissions
        try:
            os.makedirs(neptune_dir, exist_ok=True)
            # Set permissions to be writable
            os.chmod(neptune_dir, 0o755)
        except Exception as e:
            logger.error(f"Failed to create Neptune directory: {e}")
            return output_dir
        
        # Schema path
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), schema_file)
        logger.info(f"Converting from: {latest_dir}")
        logger.info(f"Converting to: {neptune_dir}")
        logger.info(f"Using schema: {schema_path}")
        
        try:
            neptune_output = convert_to_neptune(str(latest_dir), neptune_dir, schema_file=schema_path)
            logger.info(f"Neptune conversion complete. Files available in: {neptune_output}")
            logger.info(f"Node files use 'node_' prefix and edge files use 'edges_' prefix.")
            neptune_output_dir = neptune_output
            
            # Set permissions on created files
            import glob
            for file_path in glob.glob(os.path.join(neptune_dir, "*")):
                try:
                    os.chmod(file_path, 0o644)
                except:
                    pass  # Ignore permission errors
                    
        except Exception as e:
            logger.error(f"Neptune conversion failed: {e}")
            import traceback
            traceback.print_exc()
            neptune_output_dir = None
    
    # Upload to S3 if requested
    s3_uris = []
    if upload_to_s3 and neptune_output_dir and s3_bucket:
        try:
            from utils.s3_uploader import S3Uploader
            logger.info(f"\nUploading Neptune files to S3 bucket {s3_bucket}...")
            
            # Create S3 prefix with timestamp if not provided
            if not s3_prefix:
                timestamp = time.strftime("%Y%m%d%H%M%S")
                s3_prefix = f"enrichr_kg/{timestamp}"
            
            # Upload files
            uploader = S3Uploader(s3_bucket)
            s3_uris = uploader.upload_directory(neptune_output_dir, s3_prefix)
            
            if s3_uris:
                logger.info(f"Successfully uploaded {len(s3_uris)} files to S3")
                logger.info(f"S3 prefix: s3://{s3_bucket}/{s3_prefix}/")
            else:
                logger.error("Failed to upload files to S3")
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            import traceback
            traceback.print_exc()
    
    # Load to Neptune if requested
    if load_to_neptune and s3_uris and neptune_endpoint:
        try:
            from utils.neptune_loader import NeptuneLoader
            logger.info(f"\nLoading data into Neptune database at {neptune_endpoint}...")
            logger.info("Using ordered loading: nodes first, then edges...")
            
            # Create Neptune loader
            loader = NeptuneLoader(neptune_endpoint, iam_role_arn)
            
            # Use ordered loading to ensure nodes are loaded before edges
            s3_source_uri = f"s3://{s3_bucket}/{s3_prefix}/"
            results = loader.start_ordered_load_job(
                s3_source_uri,
                load_format="csv",
                fail_on_error=False,  # Continue loading even if some files fail
                parallelism="MEDIUM",
                poll_interval=15,     # Check status every 15 seconds
                timeout=1800          # 30 minute timeout per file
            )
            
            if results["status"] == "success":
                total_jobs = len(results["node_jobs"]) + len(results["edge_jobs"])
                logger.info(f"✅ Neptune ordered load completed successfully!")
                logger.info(f"   Node files loaded: {len(results['node_jobs'])}")
                logger.info(f"   Edge files loaded: {len(results['edge_jobs'])}")
                logger.info(f"   Total jobs: {total_jobs}")
                
                if results["errors"]:
                    logger.warning(f"Encountered {len(results['errors'])} warnings:")
                    for error in results["errors"][:5]:  # Show first 5 errors
                        logger.warning(f"  - {error}")
                    if len(results["errors"]) > 5:
                        logger.warning(f"  ... and {len(results['errors']) - 5} more")
            else:
                logger.error(f"❌ Neptune ordered load failed!")
                logger.error(f"Errors: {results.get('errors', [])}")
                
        except Exception as e:
            logger.error(f"Error loading to Neptune: {e}")
            import traceback
            traceback.print_exc()
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("Enrichr Knowledge Graph Build Complete!")
    logger.info("=" * 60)
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Total nodes: {len(all_nodes):,}")
    logger.info(f"Total edges: {len(all_edges):,}")
    
    return output_dir

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Build Enrichr Knowledge Graph")
    parser.add_argument("--output-dir", "-o", help="Output directory for the knowledge graph")
    parser.add_argument("--datasets", "-d", nargs="+", help="Datasets to include (default: all)")
    parser.add_argument("--neptune", "-n", action="store_true", help="Convert output to Neptune format")
    parser.add_argument("--config", "-c", help="Path to datasets config file")
    
    # S3 upload arguments
    parser.add_argument("--upload-s3", action="store_true", help="Upload Neptune files to S3")
    parser.add_argument("--s3-bucket", help="S3 bucket name for upload")
    parser.add_argument("--s3-prefix", help="S3 prefix for uploaded files")
    
    # Neptune loading arguments
    parser.add_argument("--load-neptune", action="store_true", help="Load data into Neptune database")
    parser.add_argument("--neptune-endpoint", help="Neptune endpoint URL")
    parser.add_argument("--iam-role-arn", help="IAM role ARN for Neptune to access S3")
    
    args = parser.parse_args()
    
    try:
        output_dir = build_enrichr_knowledge_graph(
            output_dir=args.output_dir,
            datasets=args.datasets,
            convert_to_neptune_format=args.neptune,
            upload_to_s3=args.upload_s3,
            s3_bucket=args.s3_bucket,
            s3_prefix=args.s3_prefix,
            load_to_neptune=args.load_neptune,
            neptune_endpoint=args.neptune_endpoint,
            iam_role_arn=args.iam_role_arn,
            config_path=args.config
        )
        
        logger.info("\n" + "=" * 60)
        logger.info("Enrichr Knowledge Graph Built Successfully!")
        logger.info("=" * 60)
        logger.info(f"Output available in: {output_dir}")
        
    except Exception as e:
        logger.error(f"Error building knowledge graph: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
