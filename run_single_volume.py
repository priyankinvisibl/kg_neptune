#!/usr/bin/env python3
"""
Single Volume Mount Knowledge Graph Builder
Clean, production-ready version
"""

import sys
import os
import argparse
import shutil
from pathlib import Path
from enrichr_kg_builder import build_enrichr_knowledge_graph_from_config

def setup_workspace():
    """Setup workspace directories with proper permissions"""
    workspace_dirs = [
        '/workspace/config',
        '/workspace/output', 
        '/workspace/biocypher-out',
        '/workspace/neptune',
        '/workspace/logs'
    ]
    
    for directory in workspace_dirs:
        os.makedirs(directory, exist_ok=True)
        os.chmod(directory, 0o755)
    
    print("Workspace directories created")

def convert_to_neptune_format(biocypher_dir, neptune_dir):
    """Convert BioCypher output to Neptune format"""
    try:
        from utils.neptune_converter import convert_to_neptune
        
        # Ensure neptune directory exists and is writable
        os.makedirs(neptune_dir, exist_ok=True)
        os.chmod(neptune_dir, 0o755)
        
        # Find schema file
        schema_file = '/app/config/schema_enrichr.yaml'
        if not os.path.exists(schema_file):
            schema_file = None
        
        print(f"Converting to Neptune format...")
        print(f"  From: {biocypher_dir}")
        print(f"  To: {neptune_dir}")
        
        result = convert_to_neptune(biocypher_dir, neptune_dir, schema_file=schema_file)
        
        if result and os.path.exists(result):
            # Count files created
            import glob
            files = glob.glob(os.path.join(result, '*'))
            print(f"Neptune conversion successful! Created {len(files)} files")
            
            # Set proper permissions on created files
            for file_path in files:
                try:
                    os.chmod(file_path, 0o644)
                except:
                    pass
            
            return True
        else:
            print("Neptune conversion failed")
            return False
            
    except Exception as e:
        print(f"Neptune conversion error: {e}")
        return False

def organize_outputs():
    """Organize all outputs in workspace"""
    try:
        # Copy biocypher-out from /app to workspace if it exists
        if os.path.exists('/app/biocypher-out') and os.listdir('/app/biocypher-out'):
            print("Copying BioCypher outputs to workspace...")
            for item in os.listdir('/app/biocypher-out'):
                src = os.path.join('/app/biocypher-out', item)
                dst = os.path.join('/workspace/biocypher-out', item)
                if os.path.isdir(src):
                    if os.path.exists(dst):
                        shutil.rmtree(dst)
                    shutil.copytree(src, dst)
                else:
                    shutil.copy2(src, dst)
        
        # Find the latest BioCypher output directory and convert to Neptune
        biocypher_path = Path('/workspace/biocypher-out')
        if biocypher_path.exists():
            subdirs = [d for d in biocypher_path.iterdir() if d.is_dir() and not d.name.endswith('_neptune')]
            if subdirs:
                latest_dir = max(subdirs, key=lambda x: x.stat().st_mtime)
                print(f"Found BioCypher output: {latest_dir}")
                
                # Convert to Neptune format
                convert_to_neptune_format(str(latest_dir), '/workspace/neptune')
        
        print("All outputs organized in workspace")
        
    except Exception as e:
        print(f"Error organizing outputs: {e}")

def main():
    parser = argparse.ArgumentParser(description="Single Volume Knowledge Graph Builder")
    parser.add_argument("--config", "-c", default="/app/config/kg_config.yaml", 
                       help="Path to configuration file")
    
    args = parser.parse_args()
    
    print("=== Single Volume Knowledge Graph Builder ===")
    print(f"Config: {args.config}")
    
    # Setup workspace
    setup_workspace()
    
    # Copy config to workspace
    if not args.config.startswith('/workspace/'):
        workspace_config = '/workspace/config/kg_config.yaml'
        if os.path.exists(args.config):
            shutil.copy2(args.config, workspace_config)
            print(f"Config copied to workspace")
    
    try:
        # Run the knowledge graph builder
        result = build_enrichr_knowledge_graph_from_config(args.config)
        
        if result:
            print(f"Knowledge Graph build completed successfully!")
            
            # Organize all outputs in workspace
            organize_outputs()
            
            # Create build summary
            summary_lines = [
                "Knowledge Graph Build Summary",
                "=" * 40,
                "Build completed successfully!",
                f"Primary output: {result}"
            ]
            
            # Count files in workspace directories
            for subdir in ['output', 'biocypher-out', 'neptune', 'logs', 'config']:
                dir_path = f'/workspace/{subdir}'
                if os.path.exists(dir_path):
                    file_count = sum(len(files) for _, _, files in os.walk(dir_path))
                    if file_count > 0:
                        summary_lines.append(f"{subdir.replace('-', ' ').title()}: {file_count} files")
            
            summary_lines.append("All files organized in /workspace/")
            
            with open('/workspace/build_summary.txt', 'w') as f:
                f.write('\n'.join(summary_lines))
            
            print("=" * 50)
            print("BUILD COMPLETE - All outputs in /workspace/")
            print("=" * 50)
            
            # Show Neptune file details
            neptune_dir = '/workspace/neptune'
            if os.path.exists(neptune_dir) and os.listdir(neptune_dir):
                import glob
                node_files = glob.glob(os.path.join(neptune_dir, 'node_*.csv'))
                edge_files = glob.glob(os.path.join(neptune_dir, 'edges_*.csv'))
                print(f"Neptune files: {len(node_files)} node files, {len(edge_files)} edge files")
            
        else:
            print("Knowledge Graph build failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
