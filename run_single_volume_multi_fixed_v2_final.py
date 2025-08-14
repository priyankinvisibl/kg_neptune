#!/usr/bin/env python3
"""
Fixed Multi-Builder Single Volume Mount Knowledge Graph Builder with S3 Upload Support
FINAL VERSION: Properly handles output directory mapping and only processes current run
"""

import sys
import os
import argparse
import shutil
import yaml
import time
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global variable to track directories created in current run with their builders
CURRENT_RUN_DIRECTORIES = {}  # Changed to dict: {directory_path: builder_name}

def setup_workspace():
    """Setup workspace directories with proper permissions"""
    workspace_dirs = [
        '/workspace/config',
        '/workspace/biocypher-out',
        '/workspace/neptune',
        '/workspace/logs'
    ]
    
    for directory in workspace_dirs:
        os.makedirs(directory, exist_ok=True)
        os.chmod(directory, 0o755)
    
    print("Workspace directories created")

def load_config(config_path):
    """Load and parse the configuration file"""
    try:
        # If it's a relative path like "config/kg_config_s3.yaml", 
        # try workspace first, then container
        if not config_path.startswith('/'):
            workspace_path = f"/workspace/{config_path}"
            container_path = f"/app/{config_path}"
            
            if os.path.exists(workspace_path):
                config_path = workspace_path
                print(f"Using config from workspace: {config_path}")
            elif os.path.exists(container_path):
                config_path = container_path
                print(f"Using config from container: {config_path}")
            else:
                print(f"Config file not found: {config_path}")
                return None
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Debug: Print loaded config structure
        general_config = config.get('general', {})
        print(f"Config builders: {general_config.get('builders', 'NOT FOUND')}")
        print(f"Config convert_to_neptune: {general_config.get('convert_to_neptune', 'NOT FOUND')}")
        
        s3_config = config.get('s3', {})
        print(f"Config S3 upload: {s3_config.get('upload', 'NOT FOUND')}")
        
        return config
        
    except Exception as e:
        print(f"Error loading config: {e}")
        return None

def get_available_builders():
    """Get list of available builders"""
    return {
        "enrichr": "enrichr_kg_builder",
        "civic": "civic_kg_builder", 
        "hpo": "hpo_configurable_kg_builder",
        "dgidb": "dgidb_kg_builder",
        "mesh_nt": "mesh_nt_kg",
        "mesh_xml": "mesh_xml_kg"
    }

def copy_output_to_workspace(app_output_dir, builder_name):
    """Copy BioCypher output from /app/biocypher-out to /workspace/biocypher-out and track it"""
    global CURRENT_RUN_DIRECTORIES
    
    try:
        # Find the latest directory in app output
        app_biocypher_path = Path('/app/biocypher-out')
        if not app_biocypher_path.exists():
            print(f"No BioCypher output found in /app/biocypher-out")
            return None
            
        # Get all subdirectories and find the latest one
        subdirs = [d for d in app_biocypher_path.iterdir() if d.is_dir()]
        if not subdirs:
            print(f"No subdirectories found in /app/biocypher-out")
            return None
            
        latest_dir = max(subdirs, key=lambda x: x.stat().st_mtime)
        print(f"Found latest BioCypher output: {latest_dir}")
        
        # Copy to workspace
        workspace_target = f"/workspace/biocypher-out/{latest_dir.name}"
        if os.path.exists(workspace_target):
            shutil.rmtree(workspace_target)
        
        shutil.copytree(str(latest_dir), workspace_target)
        print(f"Copied {latest_dir} -> {workspace_target}")
        
        # Track this directory for Neptune conversion with builder association
        CURRENT_RUN_DIRECTORIES[workspace_target] = builder_name
        print(f"Tracked current run directory: {workspace_target} -> {builder_name}")
        
        return workspace_target
        
    except Exception as e:
        print(f"Error copying output to workspace: {e}")
        return None

def run_builder(builder_name, module_name, config, builder_output_dir):
    """Run a specific builder with the given configuration"""
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Running {builder_name} builder")
    logger.info(f"{'=' * 60}")
    
    try:
        start_time = time.time()
        
        if module_name == "enrichr_kg_builder":
            # Import and run Enrichr builder
            import enrichr_kg_builder
            
            # Get configuration sections
            s3_config = config.get('s3', {})
            neptune_config = config.get('neptune', {})
            general_config = config.get('general', {})
            
            # Debug: Print what we're passing to the builder
            print(f"Passing to enrichr builder:")
            print(f"  convert_to_neptune_format: False")  # We'll handle this in multi-builder
            print(f"  upload_to_s3: False")  # We'll handle this in multi-builder
            print(f"  load_to_neptune: {neptune_config.get('load', False)}")
            
            # Disable Neptune conversion and S3 upload in individual builder
            # We'll handle these in the multi-builder runner
            result = enrichr_kg_builder.build_enrichr_knowledge_graph(
                output_dir=builder_output_dir,
                convert_to_neptune_format=False,  # Handle in multi-builder
                upload_to_s3=False,  # Handle in multi-builder
                s3_bucket=None,
                s3_prefix=None,
                load_to_neptune=False,  # Handle after S3 upload
                neptune_endpoint=None,
                iam_role_arn=None,
                config=config
            )
            
            # Copy output to workspace and track it
            workspace_dir = copy_output_to_workspace(builder_output_dir, builder_name)
            
        elif module_name == "civic_kg_builder":
            import civic_kg_builder
            
            print(f"Passing to civic builder:")
            print(f"  convert_to_neptune_format: False")  # Handle in multi-builder
            print(f"  download_data: True")  # Enable downloads from URLs
            print(f"  config: {config is not None}")
            
            result = civic_kg_builder.build_civic_knowledge_graph(
                output_dir=None,  # Use default BioCypher behavior like others
                convert_to_neptune_format=False,  # Handle in multi-builder
                download_data=True,  # Enable downloads from URLs
                config=config  # Pass config for URL access
            )
            
            workspace_dir = copy_output_to_workspace(builder_output_dir, builder_name)
            
        elif module_name == "hpo_configurable_kg_builder":
            import hpo_configurable_kg_builder
            
            print(f"Passing to hpo builder:")
            print(f"  output_dir: {builder_output_dir}")
            print(f"  convert_to_neptune_format: False")  # Handle in multi-builder
            print(f"  config_path: /app/config/hpo_column_config.yaml")
            
            # HPO builder needs a config file
            hpo_config_path = "/app/config/hpo_column_config.yaml"
            if not os.path.exists(hpo_config_path):
                hpo_config_path = "config/hpo_column_config.yaml"
            
            # Check if HPO data files exist
            hpo_data_files = [
                'hpo/genes_to_disease.txt',
                'hpo/phenotype_to_genes.txt', 
                'hpo/phenotype.hpoa'
            ]
            
            missing_files = [f for f in hpo_data_files if not os.path.exists(f)]
            if missing_files:
                print(f"⚠️  HPO data files missing: {missing_files}")
                print(f"HPO builder will run but may produce empty output")
            
            result = hpo_configurable_kg_builder.build_hpo_knowledge_graph(
                config_path=hpo_config_path,
                output_dir=builder_output_dir,
                convert_to_neptune_format=False,  # Handle in multi-builder
                main_config=config  # Pass main config for URL downloads
            )
            
            workspace_dir = copy_output_to_workspace(builder_output_dir, builder_name)
            
        elif module_name == "dgidb_kg_builder":
            import dgidb_kg_builder
            
            print(f"Passing to dgidb builder:")
            print(f"  convert_to_neptune_format: False")  # Handle in multi-builder
            print(f"  download_data: True")  # Enable downloads from URLs
            print(f"  config: {config is not None}")
            
            result = dgidb_kg_builder.build_dgidb_knowledge_graph(
                output_dir=None,  # Use default BioCypher behavior
                convert_to_neptune_format=False,  # Handle in multi-builder
                download_data=True,  # Enable downloads from URLs
                config=config  # Pass config for URL access
            )
            
            workspace_dir = copy_output_to_workspace(builder_output_dir, builder_name)
            
        elif module_name == "mesh_nt_kg":
            import mesh_nt_kg
            
            print(f"Passing to mesh_nt builder:")
            print(f"  output_dir: {builder_output_dir}")
            print(f"  convert_to_neptune_format: False")  # Handle in multi-builder
            
            result = mesh_nt_kg.build_mesh_nt_knowledge_graph(
                output_dir=builder_output_dir,
                convert_to_neptune_format=False  # Handle in multi-builder
            )
            
            workspace_dir = copy_output_to_workspace(builder_output_dir, builder_name)
            
        elif module_name == "mesh_xml_kg":
            import mesh_xml_kg
            
            print(f"Passing to mesh_xml builder:")
            print(f"  output_dir: {builder_output_dir}")
            print(f"  convert_to_neptune_format: False")  # Handle in multi-builder
            
            result = mesh_xml_kg.build_mesh_xml_knowledge_graph(
                output_dir=builder_output_dir,
                convert_to_neptune_format=False  # Handle in multi-builder
            )
            
            workspace_dir = copy_output_to_workspace(builder_output_dir, builder_name)
            
        else:
            raise ValueError(f"Unknown builder: {builder_name}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        return {
            'builder': builder_name,
            'status': 'success',
            'duration': duration,
            'output_dir': builder_output_dir,
            'workspace_dir': workspace_dir,
            'result': result
        }
        
    except Exception as e:
        logger.error(f"Error running {builder_name}: {e}")
        return {
            'builder': builder_name,
            'status': 'failed',
            'error': str(e),
            'output_dir': builder_output_dir
        }

def convert_to_neptune_format(biocypher_dir, neptune_dir, builder_name=""):
    """Convert BioCypher output to Neptune format"""
    try:
        from utils.neptune_converter import convert_to_neptune
        
        # Ensure neptune directory exists and is writable
        os.makedirs(neptune_dir, exist_ok=True)
        os.chmod(neptune_dir, 0o755)
        
        print(f"Converting {biocypher_dir} to Neptune format...")
        print(f"  Source: {biocypher_dir}")
        print(f"  Target: {neptune_dir}")
        
        # Find schema file
        schema_file = None
        schema_paths = [
            "/app/config/schema_enrichr.yaml",
            "config/schema_enrichr.yaml",
            "/app/config/schema_civic.yaml", 
            "config/schema_civic.yaml",
            "/app/config/schema_hpo.yaml",
            "config/schema_hpo.yaml",
            "/app/config/schema_dgidb.yaml",
            "config/schema_dgidb.yaml",
            "/app/config/schema_mesh.yaml",
            "config/schema_mesh.yaml"
        ]
        
        for path in schema_paths:
            if os.path.exists(path):
                schema_file = path
                break
        
        print(f"  Schema: {schema_file}")
        
        result = convert_to_neptune(biocypher_dir, neptune_dir, schema_file=schema_file)
        
        if result and os.path.exists(result):
            print(f"✅ Neptune conversion successful: {result}")
            return result
        else:
            print(f"❌ Neptune conversion failed")
            return None
            
    except Exception as e:
        print(f"Error converting to Neptune format: {e}")
        return None

def upload_to_s3(neptune_dir, s3_config, builder_name, timestamp=None):
    """Upload Neptune files to S3"""
    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError
        
        s3_bucket = s3_config.get('bucket')
        s3_prefix = s3_config.get('prefix', '')
        
        if not s3_bucket:
            print("No S3 bucket specified")
            return [], None
        
        # Use provided timestamp or generate new one
        if not timestamp:
            timestamp = time.strftime("%Y%m%d%H%M%S")
            print(f"Generated new timestamp: {timestamp}")
        else:
            print(f"Using provided timestamp: {timestamp}")
        
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        logger.info(f"\nUploading {builder_name} Neptune files to S3 bucket {s3_bucket}...")
        
        # Create S3 prefix with timestamp if not provided
        if not s3_prefix:
            s3_prefix = f"{builder_name}_kg/{timestamp}"
        else:
            # Add builder name and timestamp to prefix
            s3_prefix = f"{s3_prefix}/{builder_name}/{timestamp}"
        
        print(f"S3 prefix for upload: {s3_prefix}")
        
        # Upload files
        uploaded_uris = []
        neptune_path = Path(neptune_dir)
        
        if neptune_path.exists():
            for file_path in neptune_path.rglob('*'):
                if file_path.is_file():
                    # Calculate relative path for S3 key
                    relative_path = file_path.relative_to(neptune_path)
                    s3_key = f"{s3_prefix}/{relative_path}"
                    
                    try:
                        s3_client.upload_file(str(file_path), s3_bucket, s3_key)
                        s3_uri = f"s3://{s3_bucket}/{s3_key}"
                        uploaded_uris.append(s3_uri)
                        print(f"✅ Uploaded: {s3_uri}")
                    except ClientError as e:
                        print(f"❌ Failed to upload {file_path}: {e}")
        
        logger.info(f"Uploaded {len(uploaded_uris)} files to S3")
        return uploaded_uris, s3_prefix
        
    except NoCredentialsError:
        print("❌ AWS credentials not found")
        return [], None
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return [], None

def load_to_neptune(s3_uris, s3_config, neptune_config, builder_name, s3_prefix=None):
    """Load data from S3 to Neptune with proper ordering (nodes first, then edges)"""
    try:
        from utils.neptune_loader_sdk import NeptuneLoaderSDK
        
        neptune_endpoint = neptune_config.get('endpoint')
        iam_role_arn = neptune_config.get('iam_role_arn')
        s3_bucket = s3_config.get('bucket')
        
        if not all([neptune_endpoint, iam_role_arn, s3_bucket]):
            print("Missing Neptune configuration")
            return
        
        # Extract region from Neptune endpoint or use default
        region = "us-east-1"  # Default region
        if "ap-southeast-1" in neptune_endpoint:
            region = "ap-southeast-1"
        elif "us-west-2" in neptune_endpoint:
            region = "us-west-2"
        elif "eu-west-1" in neptune_endpoint:
            region = "eu-west-1"
        
        loader = NeptuneLoaderSDK(neptune_endpoint, iam_role_arn, region)
        
        # Use the S3 prefix from upload function to construct consistent S3 directory URI
        if s3_prefix:
            s3_source_uri = f"s3://{s3_bucket}/{s3_prefix}/"
            print(f"Using S3 prefix from upload: {s3_prefix}")
        else:
            # Fallback to old method if s3_prefix not provided
            timestamp = time.strftime("%Y%m%d%H%M%S")
            config_prefix = s3_config.get('prefix', '')
            if not config_prefix:
                s3_source_uri = f"s3://{s3_bucket}/{builder_name}_kg/{timestamp}/"
            else:
                s3_source_uri = f"s3://{s3_bucket}/{config_prefix}/{builder_name}/{timestamp}/"
            print(f"Generated fallback S3 URI with timestamp: {timestamp}")
        
        print(f"Final S3 source URI: {s3_source_uri}")
        
        print(f"🚀 Starting ordered Neptune load from: {s3_source_uri}")
        print("   Loading nodes first, then edges to prevent reference errors...")
        print(f"   Using AWS SDK with IAM role: {iam_role_arn}")
        
        # Use the new SDK-based ordered loading method
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
            print(f"✅ Neptune ordered load completed successfully!")
            print(f"   Node files loaded: {len(results['node_jobs'])}")
            print(f"   Edge files loaded: {len(results['edge_jobs'])}")
            print(f"   Total jobs: {total_jobs}")
            
            if results["errors"]:
                print(f"⚠️  Encountered {len(results['errors'])} warnings:")
                for error in results["errors"][:5]:  # Show first 5 errors
                    print(f"     - {error}")
                if len(results["errors"]) > 5:
                    print(f"     ... and {len(results['errors']) - 5} more")
        else:
            print(f"❌ Neptune ordered load failed!")
            print(f"   Errors: {results.get('errors', [])}")
            
    except Exception as e:
        print(f"Error loading to Neptune: {e}")
        import traceback
        traceback.print_exc()

def organize_outputs(builder_results, config):
    """Organize all outputs in workspace and handle S3/Neptune operations"""
    try:
        global CURRENT_RUN_DIRECTORIES
        
        print("\n" + "="*60)
        print("ORGANIZING OUTPUTS")
        print("="*60)
        
        # Get configuration
        general_config = config.get('general', {})
        s3_config = config.get('s3', {})
        neptune_config = config.get('neptune', {})
        
        convert_to_neptune = general_config.get('convert_to_neptune', False)
        upload_to_s3_enabled = s3_config.get('upload', False)
        load_to_neptune_enabled = neptune_config.get('load', False)
        
        print(f"Convert to Neptune: {convert_to_neptune}")
        print(f"Upload to S3: {upload_to_s3_enabled}")
        print(f"Load to Neptune: {load_to_neptune_enabled}")
        
        all_s3_uris = []
        
        if convert_to_neptune or upload_to_s3_enabled:
            print(f"\nProcessing directories created in current run:")
            print(f"Current run directories: {CURRENT_RUN_DIRECTORIES}")
            
            # Generate a single timestamp for this entire run
            run_timestamp = time.strftime("%Y%m%d%H%M%S")
            print(f"Using consistent timestamp for this run: {run_timestamp}")
            
            # Process only directories created in current run
            for subdir_path, builder_name in CURRENT_RUN_DIRECTORIES.items():
                subdir = Path(subdir_path)
                
                if not subdir.exists():
                    print(f"⚠️  Directory not found: {subdir}")
                    continue
                
                print(f"Processing BioCypher output: {subdir} (builder: {builder_name})")
                
                # Create builder-specific neptune subdirectory
                neptune_subdir = f'/workspace/neptune/{builder_name}_{subdir.name}'
                
                # Convert to Neptune format
                neptune_result = convert_to_neptune_format(str(subdir), neptune_subdir, builder_name)
                
                if neptune_result and upload_to_s3_enabled:
                    # Upload to S3 with consistent timestamp
                    s3_uris, s3_prefix = upload_to_s3(neptune_result, s3_config, builder_name, run_timestamp)
                    all_s3_uris.extend(s3_uris)
                    
                    if s3_uris and load_to_neptune_enabled and s3_prefix:
                        # Load to Neptune using the same S3 prefix
                        load_to_neptune(s3_uris, s3_config, neptune_config, builder_name, s3_prefix)
        
        print("All outputs organized in workspace")
        
        # Return S3 URIs for summary
        return all_s3_uris
        
    except Exception as e:
        print(f"Error organizing outputs: {e}")
        return []

def create_build_summary(builder_results, config, s3_uris=None):
    """Create a comprehensive build summary"""
    try:
        general_config = config.get('general', {})
        s3_config = config.get('s3', {})
        neptune_config = config.get('neptune', {})
        
        summary_lines = [
            "Multi-Builder Knowledge Graph Build Summary",
            "=" * 50,
            f"Build completed at: {time.strftime('%Y-%m-%d %H:%M:%S')}",
            ""
        ]
        
        # Add configuration info
        summary_lines.extend([
            "Configuration:",
            f"  Requested Builders: {general_config.get('builders', [])}",
            f"  Convert to Neptune: {general_config.get('convert_to_neptune', False)}",
            f"  S3 Upload: {s3_config.get('upload', False)}",
            f"  S3 Bucket: {s3_config.get('bucket', 'N/A')}",
            f"  Neptune Load: {neptune_config.get('load', False)}",
            f"  Neptune Endpoint: {neptune_config.get('endpoint', 'N/A')}",
            ""
        ])
        
        # Add builder results
        summary_lines.append("Builder Results:")
        for result in builder_results:
            status = result.get('status', 'unknown')
            builder = result.get('builder', 'unknown')
            duration = result.get('duration', 0)
            
            if status == 'success':
                summary_lines.append(f"  ✅ {builder}: SUCCESS ({duration:.2f}s)")
                workspace_dir = result.get('workspace_dir')
                if workspace_dir:
                    summary_lines.append(f"     Output: {workspace_dir}")
            else:
                error = result.get('error', 'Unknown error')
                summary_lines.append(f"  ❌ {builder}: FAILED - {error}")
        
        summary_lines.append("")
        
        # Add S3 info if available
        if s3_uris:
            summary_lines.extend([
                f"S3 Uploads ({len(s3_uris)} files):",
                *[f"  - {uri}" for uri in s3_uris[:10]],  # Show first 10
                *([f"  ... and {len(s3_uris) - 10} more"] if len(s3_uris) > 10 else []),
                ""
            ])
        
        # Add workspace info
        summary_lines.extend([
            "Output Locations:",
            f"  BioCypher Output: /workspace/biocypher-out/",
            f"  Neptune Format: /workspace/neptune/",
            f"  Logs: /workspace/logs/",
            f"  Config: /workspace/config/",
            ""
        ])
        
        return "\n".join(summary_lines)
        
    except Exception as e:
        return f"Error creating build summary: {e}"

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Multi-Builder Knowledge Graph Builder')
    parser.add_argument('--config', required=True, help='Configuration file path')
    args = parser.parse_args()
    
    print("=" * 60)
    print("MULTI-BUILDER KNOWLEDGE GRAPH BUILDER v2 (FINAL)")
    print("=" * 60)
    
    # Setup workspace
    setup_workspace()
    
    # Load configuration
    config = load_config(args.config)
    if not config:
        print("Failed to load configuration. Exiting.")
        sys.exit(1)
    
    # Copy config to workspace
    if not args.config.startswith('/workspace/'):
        workspace_config = '/workspace/config/kg_config.yaml'
        if os.path.exists(args.config):
            shutil.copy2(args.config, workspace_config)
            print(f"Config copied to workspace")
    
    # Get builders from config - STRICT parsing
    general_config = config.get('general', {})
    builders_to_run = general_config.get('builders', ['enrichr'])
    convert_to_neptune = general_config.get('convert_to_neptune', False)
    
    # Validate builders_to_run is a list
    if not isinstance(builders_to_run, list):
        print(f"ERROR: 'builders' must be a list, got: {type(builders_to_run)}")
        sys.exit(1)
    
    print(f"Builders to run: {builders_to_run}")
    print(f"Convert to Neptune: {convert_to_neptune}")
    
    # Get available builders
    available_builders = get_available_builders()
    
    # Filter valid builders - STRICT filtering
    valid_builders = {}
    for builder in builders_to_run:
        if builder in available_builders:
            valid_builders[builder] = available_builders[builder]
            print(f"✅ Builder '{builder}' is valid")
        else:
            print(f"❌ Unknown builder: '{builder}' (available: {list(available_builders.keys())})")
    
    if not valid_builders:
        print("No valid builders found in configuration. Exiting.")
        sys.exit(1)
    
    print(f"Final valid builders: {list(valid_builders.keys())}")
    
    # Run each builder
    builder_results = []
    for builder_name, module_name in valid_builders.items():
        print(f"\n🚀 Starting builder: {builder_name}")
        
        # Create builder-specific output directory
        builder_output_dir = f"/app/output/{builder_name}"
        os.makedirs(builder_output_dir, exist_ok=True)
        
        # Run the builder
        result = run_builder(builder_name, module_name, config, builder_output_dir)
        builder_results.append(result)
        
        print(f"✅ Builder {builder_name} completed with status: {result.get('status')}")
    
    # Check if any builders succeeded
    successful_builds = [r for r in builder_results if r.get('status') == 'success']
    
    if successful_builds:
        print(f"\n🎉 {len(successful_builds)} builders completed successfully!")
        
        # Organize all outputs in workspace (includes S3 upload and Neptune loading)
        s3_uris = organize_outputs(builder_results, config)
        
        # Create comprehensive build summary
        summary = create_build_summary(builder_results, config, s3_uris)
        
        with open('/workspace/build_summary.txt', 'w') as f:
            f.write(summary)
        
        print("=" * 60)
        print("MULTI-BUILDER BUILD COMPLETE")
        print("=" * 60)
        print(summary)
        
    else:
        print("❌ All builders failed!")
        # Still create summary for debugging
        summary = create_build_summary(builder_results, config)
        with open('/workspace/build_summary.txt', 'w') as f:
            f.write(summary)
        print(summary)
        sys.exit(1)

if __name__ == "__main__":
    main()
