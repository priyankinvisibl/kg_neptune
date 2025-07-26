#!/usr/bin/env python3
"""
Simple entry point for running knowledge graph builder from config file
"""

import sys
import os
import argparse
from enrichr_kg_builder import build_enrichr_knowledge_graph_from_config

def main():
    parser = argparse.ArgumentParser(description="Run Knowledge Graph Builder from Config")
    parser.add_argument("--config", "-c", default="/app/config/kg_config.yaml", 
                       help="Path to configuration file")
    
    args = parser.parse_args()
    
    print(f"Starting Knowledge Graph Builder with config: {args.config}")
    
    try:
        result = build_enrichr_knowledge_graph_from_config(args.config)
        if result:
            print(f"Knowledge Graph build completed successfully!")
            print(f"Output directory: {result}")
        else:
            print("Knowledge Graph build failed!")
            sys.exit(1)
    except Exception as e:
        print(f"Error running Knowledge Graph Builder: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
