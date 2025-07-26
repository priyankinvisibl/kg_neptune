#!/usr/bin/env python3
"""
HPO Knowledge Graph Builder using BioCypher with configurable column mappings
Integrates Human Phenotype Ontology data into a knowledge graph
Includes gene enhancement functionality
"""

import os
import sys
import time
import yaml
import csv
import pandas as pd
import argparse
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from biocypher import BioCypher
from adapters.hpo.hpo_configurable_adapter import HPOConfigurableAdapter
from utils.neptune_converter import convert_to_neptune

def load_ncbi_gene_data(ncbi_file_path):
    """Load NCBI gene data and create a lookup dictionary by gene symbol"""
    logger.info(f"Loading NCBI gene data from {ncbi_file_path}...")
    
    if not os.path.exists(ncbi_file_path):
        logger.warning(f"Warning: NCBI file not found at {ncbi_file_path}")
        logger.warning("Gene enhancement will be skipped.")
        return {}
    
    try:
        # Determine file type based on extension
        file_ext = os.path.splitext(ncbi_file_path)[1].lower()
        
        if file_ext == '.xlsx':
            # Load Excel file
            df = pd.read_excel(ncbi_file_path)
        elif file_ext in ['.tsv', '.txt']:
            # Load TSV file
            df = pd.read_csv(ncbi_file_path, sep='\t')
        elif file_ext == '.csv':
            # Load CSV file
            df = pd.read_csv(ncbi_file_path)
        else:
            logger.warning(f"Unsupported file format: {file_ext}")
            logger.warning("Gene enhancement will be skipped.")
            return {}
        
        logger.info(f"Loaded {len(df)} genes from NCBI data")
        
        # Check for required columns with case-insensitive matching
        columns = [col.lower() for col in df.columns]
        
        # Find symbol column
        symbol_col = None
        for col_name in ['symbol', 'gene_symbol', 'gene symbol', 'symbol_name']:
            if col_name in columns:
                symbol_col = df.columns[columns.index(col_name)]
                break
        
        if not symbol_col:
            logger.error("Error: Could not find Symbol column in NCBI data")
            logger.warning("Gene enhancement will be skipped.")
            return {}
            
        # Find description column
        desc_col = None
        for col_name in ['description', 'desc', 'gene_description', 'gene description']:
            if col_name in columns:
                desc_col = df.columns[columns.index(col_name)]
                break
        
        if not desc_col:
            logger.warning("Warning: Could not find description column in NCBI data")
            logger.warning("Using empty descriptions")
            desc_col = None
            
        # Find type_of_gene column
        type_col = None
        for col_name in ['type_of_gene', 'type of gene', 'gene_type', 'gene type']:
            if col_name in columns:
                type_col = df.columns[columns.index(col_name)]
                break
        
        if not type_col:
            logger.warning("Warning: Could not find type_of_gene column in NCBI data")
            logger.warning("Using empty gene types")
            type_col = None
        
        # Create lookup dictionary by Symbol
        gene_lookup = {}
        for _, row in df.iterrows():
            symbol = str(row[symbol_col]).strip() if symbol_col else ''
            if symbol and symbol != 'nan':
                gene_lookup[symbol] = {
                    'description': str(row[desc_col]) if desc_col and pd.notna(row[desc_col]) else '',
                    'type_of_gene': str(row[type_col]) if type_col and pd.notna(row[type_col]) else ''
                }
        
        logger.info(f"Created lookup for {len(gene_lookup)} unique gene symbols")
        return gene_lookup
    
    except Exception as e:
        logger.error(f"Error loading NCBI data: {e}")
        import traceback
        traceback.print_exc()
        logger.warning("Gene enhancement will be skipped.")
        return {}

def enhance_gene_file(gene_file_path, gene_lookup):
    """Enhance gene file with NCBI data in-place (excluding synonyms)"""
    if not gene_lookup:
        logger.info("Skipping gene enhancement (no NCBI data available)")
        return
    
    logger.info(f"Enhancing gene file: {gene_file_path}")
    
    # Read the original file
    rows = []
    with open(gene_file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            rows.append(row)
    
    if not rows:
        logger.warning("No data found in gene file")
        return
    
    logger.info(f"Original format: {len(rows[0])} columns")
    
    # Enhance gene nodes with NCBI data
    enhanced_rows = []
    genes_found = 0
    genes_not_found = 0
    
    for row in rows:
        if len(row) >= 4:
            gene_symbol = row[0].strip()
            
            # Look up gene in NCBI data
            if gene_symbol in gene_lookup:
                gene_data = gene_lookup[gene_symbol]
                # Add NCBI properties (properly quoted for CSV) - excluding synonyms
                enhanced_row = row + [
                    f'"{gene_data["description"]}"' if gene_data["description"] else '',
                    f'"{gene_data["type_of_gene"]}"' if gene_data["type_of_gene"] else ''
                ]
                genes_found += 1
            else:
                # Add empty NCBI properties
                enhanced_row = row + ['""', '""']
                genes_not_found += 1
            
            enhanced_rows.append(enhanced_row)
        else:
            enhanced_rows.append(row)
    
    # Write enhanced file back
    with open(gene_file_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerows(enhanced_rows)
    
    logger.info(f"Enhanced {len(enhanced_rows)} gene nodes")
    logger.info(f"Genes found in NCBI data: {genes_found}")
    logger.info(f"Genes not found in NCBI data: {genes_not_found}")
    logger.info(f"New format: {len(enhanced_rows[0])} columns")

def enhance_gene_header(header_file_path):
    """Update Gene header to include NCBI properties (excluding synonyms)"""
    logger.info(f"Updating gene header: {header_file_path}")
    
    # Read the original header
    with open(header_file_path, 'r', encoding='utf-8') as f:
        original_header = f.read().strip()
    
    # Add NCBI columns to header (excluding synonyms)
    new_header = original_header + "\tdescription\ttype_of_gene"
    
    # Write the new header
    with open(header_file_path, 'w', encoding='utf-8') as f:
        f.write(new_header)
    
    logger.info(f"Updated header: {new_header}")

def synchronize_schema_with_config(column_config_path, schema_config_path):
    """
    Synchronize the schema configuration with the column configuration
    to ensure that the schema only includes properties that are defined in the column config
    """
    logger.info(f"Synchronizing schema with column configuration...")
    
    # Load column configuration
    with open(column_config_path, 'r') as f:
        column_config = yaml.safe_load(f)
    
    # Load schema configuration
    with open(schema_config_path, 'r') as f:
        schema_config = yaml.safe_load(f)
    
    # Map from column config sections to schema node and edge types
    config_to_schema_map = {
        'genes_to_disease': {
            'node': 'gene',
            'edge': 'gene to disease association'
        },
        'phenotype_to_genes': {
            'node': 'phenotypic feature',
            'edges': ['gene to phenotypic feature association', 'phenotypic feature to disease association']
        },
        'phenotype_hpoa': {
            'node': 'disease',
            'edge': 'disease to phenotypic feature association'
        }
    }
    
    # Track changes for reporting
    changes_made = []
    
    # Update schema properties based on column config
    for config_section, schema_types in config_to_schema_map.items():
        if config_section not in column_config:
            logger.warning(f"Warning: Section '{config_section}' not found in column config")
            continue
        
        # Synchronize node properties
        if 'node' in schema_types and schema_types['node'] in schema_config:
            node_type = schema_types['node']
            node_properties = column_config[config_section].get('node_properties', [])
            
            # Filter out any commented items (those starting with #)
            if node_properties is None:
                node_properties = []
            node_properties = [prop for prop in node_properties if not isinstance(prop, str) or not prop.strip().startswith('#')]
            
            # Convert node properties to schema format
            properties = {}
            for prop in node_properties:
                properties[prop] = 'str'
            
            # Update schema
            if 'properties' not in schema_config[node_type]:
                schema_config[node_type]['properties'] = {}
            
            old_props = schema_config[node_type].get('properties', {})
            schema_config[node_type]['properties'] = properties
            changes_made.append(f"Updated '{node_type}' properties: {old_props} -> {properties}")
            
        # Synchronize edge properties
        if 'edge' in schema_types:
            edge_type = schema_types['edge']
            edge_properties = column_config[config_section].get('edge_properties', [])
            
            # Filter out any commented items (those starting with #)
            if edge_properties is None:
                edge_properties = []
            edge_properties = [prop for prop in edge_properties if not isinstance(prop, str) or not prop.strip().startswith('#')]
            
            # Always add data_source if it's in global settings
            if 'global' in column_config and 'data_source' in column_config['global']:
                if 'data_source' not in edge_properties:
                    edge_properties.append('data_source')
            
            # Convert edge properties to schema format
            properties = {}
            for prop in edge_properties:
                properties[prop] = 'str'
            
            # Always ensure data_source is included
            if 'data_source' not in properties:
                properties['data_source'] = 'str'
            
            # Update schema
            if edge_type in schema_config:
                old_props = schema_config[edge_type].get('properties', {})
                schema_config[edge_type]['properties'] = properties
                changes_made.append(f"Updated '{edge_type}' properties: {old_props} -> {properties}")
        
        # Handle special cases for phenotype_to_genes which maps to multiple edge types
        if 'edges' in schema_types:
            for edge_type in schema_types['edges']:
                if edge_type in schema_config:
                    # Get special properties from config if available
                    special_props = column_config[config_section].get('special_properties', {})
                    
                    # Special handling for phenotype_to_genes
                    if edge_type == 'gene to phenotypic feature association':
                        # Use configurable property name or default to "via_disease"
                        via_property = special_props.get('gene_to_phenotype_via', 'via_disease')
                        old_props = schema_config[edge_type].get('properties', {})
                        schema_config[edge_type]['properties'] = {
                            via_property: 'str',
                            'data_source': 'str'
                        }
                        changes_made.append(f"Updated '{edge_type}' properties: {old_props} -> {schema_config[edge_type]['properties']}")
                    elif edge_type == 'phenotypic feature to disease association':
                        # Use configurable property name or default to "via_gene"
                        via_property = special_props.get('phenotype_to_disease_via', 'via_gene')
                        old_props = schema_config[edge_type].get('properties', {})
                        schema_config[edge_type]['properties'] = {
                            via_property: 'str',
                            'data_source': 'str'
                        }
                        changes_made.append(f"Updated '{edge_type}' properties: {old_props} -> {schema_config[edge_type]['properties']}")
    
    # Write updated schema configuration
    with open(schema_config_path, 'w') as f:
        yaml.dump(schema_config, f, default_flow_style=False)
    
    logger.info(f"Schema configuration updated at {schema_config_path}")
    
    # Report changes
    if changes_made:
        logger.info("Changes made to schema:")
        for change in changes_made:
            logger.info(f"  - {change}")
    else:
        logger.info("No changes needed to schema configuration")

def post_process_gene_file(gene_file_path, ncbi_file_path):
    """
    Post-process gene file to add NCBI data for each gene ID
    This function ensures all genes get enhanced with NCBI data
    """
    logger.info(f"\nPost-processing gene file with NCBI data: {gene_file_path}")
    
    # Load NCBI gene data
    gene_lookup = load_ncbi_gene_data(ncbi_file_path)
    if not gene_lookup:
        logger.info("Skipping post-processing (no NCBI data available)")
        return
    
    # Read the gene file
    rows = []
    with open(gene_file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            rows.append(row)
    
    if not rows:
        logger.warning("No data found in gene file")
        return
    
    logger.info(f"Original format: {len(rows[0])} columns")
    
    # Check if the file already has the NCBI columns
    has_ncbi_columns = len(rows[0]) >= 6  # Assuming standard format plus 2 NCBI columns
    if has_ncbi_columns:
        logger.info("Gene file already has NCBI columns, skipping post-processing")
        return
    
    # Enhance gene nodes with NCBI data
    enhanced_rows = []
    genes_found = 0
    genes_not_found = 0
    
    for row in rows:
        if len(row) >= 2:  # At least ID and name columns
            gene_id = row[0].strip()
            
            # Extract gene symbol from ID (if it's in a different format)
            gene_symbol = gene_id
            if ':' in gene_id:
                gene_symbol = gene_id.split(':')[-1]
            
            # Look up gene in NCBI data
            if gene_symbol in gene_lookup:
                gene_data = gene_lookup[gene_symbol]
                # Add NCBI properties (properly quoted for CSV)
                enhanced_row = row + [
                    f'"{gene_data["description"]}"' if gene_data["description"] else '',
                    f'"{gene_data["type_of_gene"]}"' if gene_data["type_of_gene"] else ''
                ]
                genes_found += 1
            else:
                # Add empty NCBI properties
                enhanced_row = row + ['', '']
                genes_not_found += 1
            
            enhanced_rows.append(enhanced_row)
        else:
            enhanced_rows.append(row)
    
    # Write enhanced file back
    with open(gene_file_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerows(enhanced_rows)
    
    logger.info(f"Enhanced {len(enhanced_rows)} gene nodes")
    logger.info(f"Genes found in NCBI data: {genes_found}")
    logger.info(f"Genes not found in NCBI data: {genes_not_found}")
    logger.info(f"New format: {len(enhanced_rows[0])} columns")
    
    # Update the header file
    gene_header_path = str(gene_file_path).replace('-part000.csv', '-header.csv')
    if os.path.exists(gene_header_path):
        enhance_gene_header(gene_header_path)
    else:
        logger.warning(f"Gene header file not found: {gene_header_path}")

def build_hpo_knowledge_graph(config_path=None, ncbi_file_path=None, output_dir=None, convert_to_neptune_format=False):
    """Build HPO knowledge graph using BioCypher with configurable column mappings"""
    
    # Check config path
    if not config_path:
        config_path = os.path.join("config", "hpo_column_config.yaml")
    
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found: {config_path}")
        return None
    
    # Synchronize schema with column configuration
    schema_config_path = os.path.join("config", "schema_config_hpo.yaml")
    synchronize_schema_with_config(config_path, schema_config_path)
    
    # Initialize BioCypher with HPO schema
    bc = BioCypher(
        schema_config_path=schema_config_path,
        output_directory=output_dir
    )
    
    logger.info("=" * 60)
    logger.info(f"Building HPO Knowledge Graph with Configurable Columns")
    logger.info(f"Using configuration: {config_path}")
    if ncbi_file_path:
        logger.info(f"With gene enhancement from: {ncbi_file_path}")
    logger.info("=" * 60)
    
    # Initialize configurable adapter
    hpo_adapter = HPOConfigurableAdapter(config_path=config_path)
    
    # Parse all HPO data
    logger.info("Parsing HPO data...")
    hpo_adapter.parse_all()
    
    # Get statistics
    stats = hpo_adapter.get_statistics()
    logger.info(f"HPO Data Statistics:")
    logger.info(f"  - Genes: {stats['genes']}")
    logger.info(f"  - Diseases: {stats['diseases']}")
    logger.info(f"  - Phenotypes: {stats['phenotypes']}")
    logger.info(f"  - Total Edges: {stats['edges']}")
    
    # Process all nodes
    logger.info("\nProcessing nodes...")
    nodes_start = time.time()
    all_nodes = list(hpo_adapter.get_nodes())
    logger.info(f"Node extraction took: {time.time() - nodes_start:.2f} seconds")
    logger.info(f"Total nodes extracted: {len(all_nodes):,}")
    
    # Process all edges
    logger.info("\nProcessing edges...")
    edges_start = time.time()
    all_edges = list(hpo_adapter.get_edges())
    logger.info(f"Edge extraction took: {time.time() - edges_start:.2f} seconds")
    logger.info(f"Total edges extracted: {len(all_edges):,}")
    
    # Write knowledge graph
    logger.info("\nWriting knowledge graph...")
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
    
    # Enhance gene files with NCBI data if provided
    if latest_dir and ncbi_file_path:
        logger.info(f"\nEnhancing gene files in {latest_dir}...")
        
        # Load NCBI gene data
        gene_lookup = load_ncbi_gene_data(ncbi_file_path)
        
        # Enhance gene data file
        gene_file = latest_dir / "Gene-part000.csv"
        if gene_file.exists():
            enhance_gene_file(gene_file, gene_lookup)
        else:
            logger.warning(f"Gene file not found: {gene_file}")
        
        # Enhance gene header file
        gene_header = latest_dir / "Gene-header.csv"
        if gene_header.exists():
            enhance_gene_header(gene_header)
        else:
            logger.warning(f"Gene header file not found: {gene_header}")
    
    # Always post-process gene files with NCBI data if provided
    # This ensures all genes get enhanced, even if they weren't enhanced during the initial processing
    if latest_dir and ncbi_file_path:
        gene_file = latest_dir / "Gene-part000.csv"
        if gene_file.exists():
            post_process_gene_file(gene_file, ncbi_file_path)
    
    # Convert to Neptune format if requested
    if convert_to_neptune_format and latest_dir:
        logger.info("\nConverting to Neptune format...")
        neptune_dir = latest_dir.parent / f"{latest_dir.name}_neptune"
        schema_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), schema_config_path)
        neptune_output = convert_to_neptune(str(latest_dir), str(neptune_dir), schema_file=schema_path)
        logger.info(f"Neptune conversion complete. Files available in: {neptune_output}")
    
    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("HPO Knowledge Graph Build Complete!")
    logger.info("=" * 60)
    logger.info(f"Output directory: {output_base}")
    
    # List output files
    if latest_dir:
        logger.info(f"\nGenerated files in {latest_dir}:")
        for file_path in sorted(latest_dir.iterdir()):
            if file_path.is_file():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                logger.info(f"  - {file_path.name}: {size_mb:.2f} MB")
    
    return output_base

def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Build HPO Knowledge Graph with configurable column mappings")
    parser.add_argument("--config", "-c", help="Path to the column configuration file")
    parser.add_argument("--ncbi", "-n", help="Path to the NCBI gene data file for gene enhancement")
    parser.add_argument("--output-dir", "-o", help="Output directory for the knowledge graph")
    parser.add_argument("--neptune", "-p", action="store_true", help="Convert output to Neptune format")
    args = parser.parse_args()
    
    try:
        output_dir = build_hpo_knowledge_graph(
            config_path=args.config,
            ncbi_file_path=args.ncbi,
            output_dir=args.output_dir,
            convert_to_neptune_format=args.neptune
        )
        
        logger.info("\n" + "=" * 60)
        logger.info("HPO Knowledge Graph Built Successfully!")
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
