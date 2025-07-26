"""
Convert Neo4j CSV files to Neptune format
"""

import pandas as pd
import os
import re
import glob
import math
import csv
import yaml
import logging

class Neo4jToNeptuneConverter:
    def __init__(self, input_dir, output_dir, batch_size=10, schema_file=None):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.schema_file = schema_file or os.path.join(os.path.dirname(input_dir), "schema_enrichr.yaml")
        self.schema = self._load_schema()
        
        # Create output directory with proper error handling
        try:
            os.makedirs(self.output_dir, exist_ok=True)
            # Set permissions to ensure writability
            os.chmod(self.output_dir, 0o755)
        except Exception as e:
            # Configure logging first
            self.logger = logging.getLogger(__name__)
            self.logger.error(f"Failed to create output directory {self.output_dir}: {e}")
            raise
        
        # Configure logging
        self.logger = logging.getLogger(__name__)
    
    def _load_schema(self):
        """Load the schema file if it exists"""
        if os.path.exists(self.schema_file):
            try:
                with open(self.schema_file, 'r') as f:
                    return yaml.safe_load(f)
            except Exception as e:
                self.logger.warning(f"Could not load schema file: {e}")
        return {}
    
    def _get_label_from_schema(self, base_name):
        """Get the appropriate label from the schema file"""
        # Try exact match first
        if base_name.lower() in self.schema:
            return base_name
        
        # Try case-insensitive match
        for key in self.schema:
            if key.lower() == base_name.lower():
                return key
            
            # Check input_label
            if self.schema[key].get('input_label', '').lower() == base_name.lower():
                return key
        
        # If no match found, return the original base_name
        return base_name

    def detect_file_type(self, header_fields):
        if ":ID" in header_fields and ":LABEL" in header_fields:
            return "vertex"
        elif (
            ":START_ID" in header_fields
            and ":END_ID" in header_fields
            and ":TYPE" in header_fields
        ):
            return "edge"
        else:
            raise ValueError(
                "Unrecognized header format: expected vertex or edge headers"
            )

    def infer_type(self, series):
        if series.isnull().all():
            return "String"

        def is_int(x):
            return re.fullmatch(r"-?\d+", x) is not None

        def is_float(x):
            return re.fullmatch(r"-?\d+\.\d*", x) is not None

        def is_bool(x):
            return x.lower() in ["true", "false"]

        def is_date(x):
            return (
                re.fullmatch(r"\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2}(Z)?)?)?", x)
                is not None
            )

        non_null = series.dropna().astype(str).str.strip(" '\"")

        if non_null.apply(is_bool).all():
            return "Bool"
        if non_null.apply(is_int).all():
            return "Int"
        if non_null.apply(is_float).all():
            return "Double"
        if non_null.apply(is_date).all():
            return "Date"
        return "String"

    def convert_headers(self, header_fields, df, file_type):
        sys_map_vertex = {":ID": "~id", ":LABEL": "~label"}
        sys_map_edge = {
            ":START_ID": "~from",
            ":END_ID": "~to",
            ":TYPE": "~label",
        }

        converted = []
        for col in header_fields:
            col = col.strip()

            # Map system fields
            if file_type == "vertex":
                if col in sys_map_vertex:
                    converted.append(sys_map_vertex[col])
                # Skip 'id' column as it's redundant with ~id for vertices
                elif col.lower() == "id":
                    continue
                else:
                    dtype = self.infer_type(df[col])
                    converted.append(f"{col}:{dtype}")
            elif file_type == "edge":
                if col in sys_map_edge:
                    converted.append(sys_map_edge[col])
                # For edges, keep the id column but map it to ~id
                elif col.lower() == "id":
                    converted.append("~id")
                else:
                    dtype = self.infer_type(df[col])
                    converted.append(f"{col}:{dtype}")
        return converted
    
    def clean_labels(self, df, headers):
        if ":LABEL" in headers:
            df[":LABEL"] = df[":LABEL"].apply(
                lambda l: l.replace("|", ";") if pd.notna(l) else ""
            )
        elif ":TYPE" in headers:
            df[":TYPE"] = df[":TYPE"].apply(
                lambda l: l.split("|")[0] if pd.notna(l) else ""
            )
        return df

    def process_batches(self, base_name, header_file, part_files):
        with open(header_file, "r", encoding="utf-8") as f:
            headers = f.readline().strip().split("\t")

        file_type = self.detect_file_type(headers)
        label = "node" if file_type == "vertex" else "edges"
        self.logger.info(f"Processing {base_name} ({file_type}) in batches of {self.batch_size}...")

        total_parts = len(part_files)
        num_batches = math.ceil(total_parts / self.batch_size)

        for i in range(num_batches):
            batch_parts = part_files[i * self.batch_size : (i + 1) * self.batch_size]

            dfs = [
                pd.read_csv(
                    pf,
                    sep="\t",
                    header=None,
                    names=headers,
                    dtype=str,
                    encoding="utf-8",
                    quoting=csv.QUOTE_MINIMAL,
                    quotechar="'",
                )
                for pf in batch_parts
            ]

            batch_df = pd.concat(dfs, ignore_index=True)
            # Apply string cleaning to each column that contains string data
            for col in batch_df.columns:
                if batch_df[col].dtype == 'object':  # Only apply to string/object columns
                    batch_df[col] = batch_df[col].map(
                        lambda x: x.strip(" '\"\t\r\n") if isinstance(x, str) else x
                    )
            batch_df = self.clean_labels(batch_df, headers)

            new_headers = self.convert_headers(headers, batch_df, file_type)
            
            # Create a new DataFrame with only the columns we want
            new_df = pd.DataFrame()
            for i, col in enumerate(new_headers):
                col_name = col.split(':')[0] if ':' in col else col
                if col_name in batch_df.columns:
                    new_df[col] = batch_df[col_name]
                else:
                    # For special columns like ~id, ~from, ~to, ~label
                    if col == "~id" and ":ID" in batch_df.columns:
                        new_df[col] = batch_df[":ID"]
                    elif col == "~id" and "id" in batch_df.columns and file_type == "edge":
                        # For edges, use the id column as ~id
                        new_df[col] = batch_df["id"]
                    elif col == "~from" and ":START_ID" in batch_df.columns:
                        new_df[col] = batch_df[":START_ID"]
                    elif col == "~to" and ":END_ID" in batch_df.columns:
                        new_df[col] = batch_df[":END_ID"]
                    elif col == "~label" and ":LABEL" in batch_df.columns:
                        if file_type == "vertex":
                            # For vertices, use the label from the schema
                            schema_label = self._get_label_from_schema(base_name)
                            new_df[col] = schema_label
                        else:
                            new_df[col] = batch_df[":LABEL"]
                    elif col == "~label" and ":TYPE" in batch_df.columns:
                        new_df[col] = batch_df[":TYPE"]
            
            # Ensure all ID columns are string
            for col in ["~id", "~from", "~to"]:
                if col in new_df.columns:
                    new_df[col] = new_df[col].astype(str)

            output_file = os.path.join(
                self.output_dir,
                f"{label}_{base_name}.csv",
            )
            
            try:
                new_df.to_csv(output_file, index=False, encoding="utf-8")
                # Set file permissions
                os.chmod(output_file, 0o644)
                self.logger.info(f"Wrote batch {i+1}/{num_batches} → {output_file}")
            except Exception as e:
                self.logger.error(f"Failed to write {output_file}: {e}")
                raise

    def process_all(self):
        header_files = glob.glob(os.path.join(self.input_dir, "*-header.csv"))

        for header_file in header_files:
            base_name = os.path.basename(header_file).replace("-header.csv", "")
            part_pattern = os.path.join(self.input_dir, f"{base_name}-part*.csv")
            part_files = sorted(glob.glob(part_pattern))

            if not part_files:
                self.logger.warning(f"No part files found for {base_name}, skipping.")
                continue

            try:
                self.process_batches(base_name, header_file, part_files)
            except Exception as e:
                self.logger.error(f"Error processing {base_name}: {e}")
                import traceback
                traceback.print_exc()
        
        return self.output_dir


def convert_to_neptune(input_dir, output_dir, batch_size=10, schema_file=None):
    """
    Convert Neo4j CSV files to Neptune format
    
    Args:
        input_dir: Directory containing Neo4j CSV files
        output_dir: Directory to write Neptune CSV files
        batch_size: Number of part files to process in each batch
        schema_file: Path to the schema file
        
    Returns:
        Path to the output directory
    """
    converter = Neo4jToNeptuneConverter(input_dir, output_dir, batch_size, schema_file)
    return converter.process_all()
