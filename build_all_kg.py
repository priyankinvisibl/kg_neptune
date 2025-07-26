#!/usr/bin/env python3
"""
Main script to build knowledge graphs from multiple data libraries
"""

import os
import sys
import argparse
import logging
import importlib
import time
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def build_knowledge_graphs(builders=None, output_dir=None, output_dirs=None, neptune=False, 
                          upload_s3=False, s3_buckets=None, s3_prefixes=None,
                          load_neptune=False, neptune_endpoint=None, iam_role_arn=None):
    """
    Build knowledge graphs from multiple data libraries
    
    Args:
        builders: List of builder modules to run (e.g., enrichr, civic)
        output_dir: Base directory for output (used if output_dirs not provided)
        output_dirs: Dictionary mapping builder names to output directories
        neptune: Whether to convert output to Neptune format
        upload_s3: Whether to upload output to S3
        s3_buckets: Dictionary mapping builder names to S3 buckets
        s3_prefixes: Dictionary mapping builder names to S3 prefixes
        load_neptune: Whether to load data into Neptune
        neptune_endpoint: Neptune endpoint URL
        iam_role_arn: IAM role ARN for Neptune to access S3
    """
    if not builders:
        logger.error("No builders specified")
        return
    
    # Available builders
    available_builders = {
        "enrichr": "enrichr_kg_builder",
        "civic": "civic_kg_builder",
        "mesh_nt": "mesh_nt_kg",
        "mesh_xml": "mesh_xml_kg",
        "hpo": "hpo_configurable_kg_builder"
    }
    
    # Filter builders
    selected_builders = {}
    for builder in builders:
        if builder in available_builders:
            selected_builders[builder] = available_builders[builder]
        else:
            logger.warning(f"Unknown builder: {builder}")
    
    if not selected_builders:
        logger.error("No valid builders selected")
        return
    
    # Create output directory if it doesn't exist
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    # Initialize output_dirs if not provided
    if not output_dirs:
        output_dirs = {}
    
    # Initialize S3 buckets and prefixes if not provided
    if upload_s3:
        if not s3_buckets:
            s3_buckets = {}
        if not s3_prefixes:
            s3_prefixes = {}
    
    # Run each builder
    results = {}
    for builder_name, module_name in selected_builders.items():
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Running {builder_name} builder")
        logger.info(f"{'=' * 60}")
        
        try:
            # For enrichr_kg_builder, use the module directly
            if module_name == "enrichr_kg_builder":
                # Import the module
                import enrichr_kg_builder
                
                # Set up builder-specific parameters
                builder_output_dir = output_dirs.get(builder_name) if builder_name in output_dirs else \
                                    os.path.join(output_dir, builder_name) if output_dir else None
                builder_s3_bucket = s3_buckets.get(builder_name) if upload_s3 else None
                builder_s3_prefix = s3_prefixes.get(builder_name) if upload_s3 else None
                
                # Run the builder
                start_time = time.time()
                result = enrichr_kg_builder.build_enrichr_knowledge_graph(
                    output_dir=builder_output_dir,
                    convert_to_neptune_format=neptune,
                    upload_to_s3=upload_s3,
                    s3_bucket=builder_s3_bucket,
                    s3_prefix=builder_s3_prefix,
                    load_to_neptune=load_neptune,
                    neptune_endpoint=neptune_endpoint,
                    iam_role_arn=iam_role_arn,
                    config_path=os.path.join("config", "datasets", "enrichr.yaml")
                )
                
                elapsed_time = time.time() - start_time
                logger.info(f"Completed {builder_name} in {elapsed_time:.2f} seconds")
                
                results[builder_name] = {
                    "output_dir": result,
                    "status": "success"
                }
            else:
                # For other builders, try to import dynamically
                try:
                    # Import the builder module
                    builder_module = importlib.import_module(module_name)
                    
                    # Get the main function from the module
                    if hasattr(builder_module, "build_knowledge_graph"):
                        build_func = builder_module.build_knowledge_graph
                    else:
                        logger.error(f"Builder {builder_name} does not have a build_knowledge_graph function")
                        continue
                    
                    # Set up builder-specific parameters
                    builder_output_dir = output_dirs.get(builder_name) if builder_name in output_dirs else \
                                        os.path.join(output_dir, builder_name) if output_dir else None
                    builder_s3_bucket = s3_buckets.get(builder_name) if upload_s3 else None
                    builder_s3_prefix = s3_prefixes.get(builder_name) if upload_s3 else None
                    
                    # Run the builder
                    start_time = time.time()
                    result = build_func(
                        output_dir=builder_output_dir,
                        convert_to_neptune_format=neptune,
                        upload_to_s3=upload_s3,
                        s3_bucket=builder_s3_bucket,
                        s3_prefix=builder_s3_prefix,
                        load_to_neptune=load_neptune,
                        neptune_endpoint=neptune_endpoint,
                        iam_role_arn=iam_role_arn
                    )
                    
                    elapsed_time = time.time() - start_time
                    logger.info(f"Completed {builder_name} in {elapsed_time:.2f} seconds")
                    
                    results[builder_name] = {
                        "output_dir": result,
                        "status": "success"
                    }
                except ImportError:
                    logger.error(f"Could not import module {module_name} for builder {builder_name}")
                    results[builder_name] = {
                        "status": "error",
                        "error": f"Module {module_name} not found"
                    }
                except Exception as e:
                    logger.error(f"Error running {builder_name} builder: {e}")
                    import traceback
                    traceback.print_exc()
                    
                    results[builder_name] = {
                        "status": "error",
                        "error": str(e)
                    }
            
        except Exception as e:
            logger.error(f"Error running {builder_name} builder: {e}")
            import traceback
            traceback.print_exc()
            
            results[builder_name] = {
                "status": "error",
                "error": str(e)
            }
    
    # Print summary
    logger.info(f"\n{'=' * 60}")
    logger.info("Knowledge Graph Build Summary")
    logger.info(f"{'=' * 60}")
    
    for builder_name, result in results.items():
        status = result["status"]
        status_str = "✅ Success" if status == "success" else "❌ Error"
        logger.info(f"{builder_name}: {status_str}")
        
        if status == "success":
            logger.info(f"  Output directory: {result['output_dir']}")
        else:
            logger.info(f"  Error: {result['error']}")
    
    return results

def parse_mapping(mapping_str):
    """Parse mapping from command line (e.g., 'key1:value1,key2:value2')"""
    if not mapping_str:
        return {}
    
    mapping = {}
    pairs = mapping_str.split(',')
    for pair in pairs:
        if ':' in pair:
            key, value = pair.split(':', 1)
            mapping[key.strip()] = value.strip()
    
    return mapping

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Build Knowledge Graphs")
    parser.add_argument("--builders", "-b", nargs="+", required=True,
                        help="Builders to run (e.g., enrichr, civic, mesh_nt, mesh_xml, hpo)")
    parser.add_argument("--output-dir", "-o", help="Base output directory")
    parser.add_argument("--output-dirs", help="Output directory mapping (e.g., 'enrichr:dir1,civic:dir2')")
    parser.add_argument("--neptune", "-n", action="store_true", help="Convert output to Neptune format")
    
    # S3 upload arguments
    parser.add_argument("--upload-s3", action="store_true", help="Upload Neptune files to S3")
    parser.add_argument("--s3-buckets", help="S3 bucket mapping (e.g., 'enrichr:bucket1,civic:bucket2')")
    parser.add_argument("--s3-prefixes", help="S3 prefix mapping (e.g., 'enrichr:prefix1,civic:prefix2')")
    
    # Neptune loading arguments
    parser.add_argument("--load-neptune", action="store_true", help="Load data into Neptune database")
    parser.add_argument("--neptune-endpoint", help="Neptune endpoint URL")
    parser.add_argument("--iam-role-arn", help="IAM role ARN for Neptune to access S3")
    
    args = parser.parse_args()
    
    # Parse mappings
    output_dirs = parse_mapping(args.output_dirs)
    s3_buckets = parse_mapping(args.s3_buckets)
    s3_prefixes = parse_mapping(args.s3_prefixes)
    
    try:
        results = build_knowledge_graphs(
            builders=args.builders,
            output_dir=args.output_dir,
            output_dirs=output_dirs,
            neptune=args.neptune,
            upload_s3=args.upload_s3,
            s3_buckets=s3_buckets,
            s3_prefixes=s3_prefixes,
            load_neptune=args.load_neptune,
            neptune_endpoint=args.neptune_endpoint,
            iam_role_arn=args.iam_role_arn
        )
        
        logger.info("\n" + "=" * 60)
        logger.info("Knowledge Graph Build Complete!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error building knowledge graphs: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
