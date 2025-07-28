"""
Utility for initiating Neptune loader jobs
"""

import logging
import requests
import json
import time
import boto3
from urllib.parse import urlparse

class NeptuneLoader:
    def __init__(self, neptune_endpoint, iam_role_arn=None, region_name="us-east-1"):
        """
        Initialize Neptune loader
        
        Args:
            neptune_endpoint: Neptune endpoint URL (e.g., https://your-neptune-endpoint:8182)
            iam_role_arn: IAM role ARN for Neptune to access S3
            region_name: AWS region name
        """
        self.neptune_endpoint = neptune_endpoint
        self.iam_role_arn = iam_role_arn
        self.region_name = region_name
        self.logger = logging.getLogger(__name__)
        
        # Ensure the endpoint has the correct format
        if not self.neptune_endpoint.startswith("https://"):
            self.neptune_endpoint = f"https://{self.neptune_endpoint}"
        
        # Add port if not present
        parsed_url = urlparse(self.neptune_endpoint)
        if not parsed_url.port:
            self.neptune_endpoint = f"{self.neptune_endpoint}:8182"
    
    def start_load_job(self, s3_uri, load_format="csv", update_single_cardinality_properties="FALSE", 
                       fail_on_error=True, parallelism="MEDIUM", mode="AUTO"):
        """
        Start a Neptune loader job
        
        Args:
            s3_uri: S3 URI of the data to load
            load_format: Format of the data (csv, opencypher)
            update_single_cardinality_properties: Whether to update single cardinality properties
            fail_on_error: Whether to fail on error
            parallelism: Parallelism level (LOW, MEDIUM, HIGH, OVERSUBSCRIBE)
            mode: Load mode (AUTO, RESUME, NEW)
            
        Returns:
            Load job ID if successful, None otherwise
        """
        loader_endpoint = f"{self.neptune_endpoint}/loader"
        
        params = {
            "source": s3_uri,
            "format": load_format,
            "updateSingleCardinalityProperties": update_single_cardinality_properties,
            "failOnError": "TRUE" if fail_on_error else "FALSE",
            "parallelism": parallelism,
            "mode": mode
        }
        
        # Add IAM role if provided
        if self.iam_role_arn:
            params["iamRoleArn"] = self.iam_role_arn
        
        try:
            response = requests.post(loader_endpoint, params=params)
            response.raise_for_status()
            
            load_id = response.json().get("payload", {}).get("loadId")
            self.logger.info(f"Started Neptune load job {load_id} for {s3_uri}")
            return load_id
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error starting Neptune load job: {e}")
            return None
    
    def get_load_status(self, load_id):
        """
        Get the status of a Neptune loader job
        
        Args:
            load_id: Load job ID
            
        Returns:
            Status dictionary if successful, None otherwise
        """
        status_endpoint = f"{self.neptune_endpoint}/loader/{load_id}"
        
        try:
            response = requests.get(status_endpoint)
            response.raise_for_status()
            
            status = response.json().get("payload", {}).get("overallStatus", {}).get("status")
            self.logger.info(f"Load job {load_id} status: {status}")
            return response.json().get("payload")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error getting load job status: {e}")
            return None
    
    def wait_for_load_completion(self, load_id, poll_interval=10, timeout=3600):
        """
        Wait for a Neptune loader job to complete
        
        Args:
            load_id: Load job ID
            poll_interval: Polling interval in seconds
            timeout: Timeout in seconds
            
        Returns:
            Final status dictionary if successful, None otherwise
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.get_load_status(load_id)
            if not status:
                return None
            
            overall_status = status.get("overallStatus", {}).get("status")
            if overall_status in ["LOAD_COMPLETED", "LOAD_COMPLETED_WITH_ERRORS"]:
                return status
            
            if overall_status in ["LOAD_FAILED", "LOAD_CANCELLED_BY_USER"]:
                self.logger.error(f"Load job {load_id} failed with status {overall_status}")
                return status
            
            time.sleep(poll_interval)
        
        self.logger.error(f"Timeout waiting for load job {load_id} to complete")
        return None
    
    def list_s3_files(self, s3_uri):
        """
        List files in an S3 location
        
        Args:
            s3_uri: S3 URI (e.g., s3://bucket/prefix/)
            
        Returns:
            List of S3 file URIs
        """
        try:
            # Parse S3 URI
            if not s3_uri.startswith('s3://'):
                raise ValueError(f"Invalid S3 URI: {s3_uri}")
            
            # Remove s3:// prefix and split bucket/key
            s3_path = s3_uri[5:]  # Remove 's3://'
            if '/' not in s3_path:
                bucket = s3_path
                prefix = ""
            else:
                bucket, prefix = s3_path.split('/', 1)
            
            # Initialize S3 client
            s3_client = boto3.client('s3')
            
            # List objects
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Skip directories (keys ending with /)
                    if not obj['Key'].endswith('/'):
                        files.append(f"s3://{bucket}/{obj['Key']}")
            
            return files
            
        except Exception as e:
            self.logger.error(f"Error listing S3 files: {e}")
            return []
    
    def start_ordered_load_job(self, s3_uri, load_format="csv", update_single_cardinality_properties="FALSE",
                              fail_on_error=True, parallelism="MEDIUM", mode="AUTO", 
                              poll_interval=10, timeout=3600):
        """
        Start Neptune loader jobs with proper ordering: nodes first, then edges
        
        Args:
            s3_uri: S3 URI of the directory containing CSV files
            load_format: Format of the data (csv, opencypher)
            update_single_cardinality_properties: Whether to update single cardinality properties
            fail_on_error: Whether to fail on error
            parallelism: Parallelism level (LOW, MEDIUM, HIGH, OVERSUBSCRIBE)
            mode: Load mode (AUTO, RESUME, NEW)
            poll_interval: Polling interval for status checks
            timeout: Timeout for each load job
            
        Returns:
            Dictionary with load results
        """
        self.logger.info(f"Starting ordered Neptune load from {s3_uri}")
        
        # List all files in the S3 location
        all_files = self.list_s3_files(s3_uri)
        if not all_files:
            self.logger.error(f"No files found in {s3_uri}")
            return {"status": "failed", "error": "No files found"}
        
        # Separate node and edge files
        node_files = [f for f in all_files if '/node_' in f and f.endswith('.csv')]
        edge_files = [f for f in all_files if '/edges_' in f and f.endswith('.csv')]
        
        self.logger.info(f"Found {len(node_files)} node files and {len(edge_files)} edge files")
        self.logger.info(f"Node files: {node_files}")
        self.logger.info(f"Edge files: {edge_files}")
        
        results = {
            "status": "success",
            "node_jobs": [],
            "edge_jobs": [],
            "errors": []
        }
        
        # Step 1: Load all node files first
        if node_files:
            self.logger.info("Step 1: Loading node files...")
            
            for node_file in node_files:
                self.logger.info(f"Loading node file: {node_file}")
                load_id = self.start_load_job(
                    node_file, load_format, update_single_cardinality_properties,
                    fail_on_error, parallelism, mode
                )
                
                if load_id:
                    # Wait for this node file to complete before proceeding
                    status = self.wait_for_load_completion(load_id, poll_interval, timeout)
                    
                    job_result = {
                        "file": node_file,
                        "load_id": load_id,
                        "status": status.get("overallStatus", {}).get("status") if status else "TIMEOUT"
                    }
                    results["node_jobs"].append(job_result)
                    
                    if not status or status.get("overallStatus", {}).get("status") not in ["LOAD_COMPLETED", "LOAD_COMPLETED_WITH_ERRORS"]:
                        error_msg = f"Node file {node_file} failed to load (job {load_id})"
                        self.logger.error(error_msg)
                        results["errors"].append(error_msg)
                        if fail_on_error:
                            results["status"] = "failed"
                            return results
                    else:
                        self.logger.info(f"✅ Node file {node_file} loaded successfully")
                else:
                    error_msg = f"Failed to start load job for node file {node_file}"
                    self.logger.error(error_msg)
                    results["errors"].append(error_msg)
                    if fail_on_error:
                        results["status"] = "failed"
                        return results
            
            self.logger.info("✅ All node files loaded successfully")
        
        # Step 2: Load all edge files after nodes are complete
        if edge_files:
            self.logger.info("Step 2: Loading edge files...")
            
            for edge_file in edge_files:
                self.logger.info(f"Loading edge file: {edge_file}")
                load_id = self.start_load_job(
                    edge_file, load_format, update_single_cardinality_properties,
                    fail_on_error, parallelism, mode
                )
                
                if load_id:
                    # Wait for this edge file to complete
                    status = self.wait_for_load_completion(load_id, poll_interval, timeout)
                    
                    job_result = {
                        "file": edge_file,
                        "load_id": load_id,
                        "status": status.get("overallStatus", {}).get("status") if status else "TIMEOUT"
                    }
                    results["edge_jobs"].append(job_result)
                    
                    if not status or status.get("overallStatus", {}).get("status") not in ["LOAD_COMPLETED", "LOAD_COMPLETED_WITH_ERRORS"]:
                        error_msg = f"Edge file {edge_file} failed to load (job {load_id})"
                        self.logger.error(error_msg)
                        results["errors"].append(error_msg)
                        if fail_on_error:
                            results["status"] = "failed"
                            return results
                    else:
                        self.logger.info(f"✅ Edge file {edge_file} loaded successfully")
                else:
                    error_msg = f"Failed to start load job for edge file {edge_file}"
                    self.logger.error(error_msg)
                    results["errors"].append(error_msg)
                    if fail_on_error:
                        results["status"] = "failed"
                        return results
            
            self.logger.info("✅ All edge files loaded successfully")
        
        # Summary
        total_jobs = len(results["node_jobs"]) + len(results["edge_jobs"])
        successful_jobs = len([j for j in results["node_jobs"] + results["edge_jobs"] 
                              if j["status"] in ["LOAD_COMPLETED", "LOAD_COMPLETED_WITH_ERRORS"]])
        
        self.logger.info(f"Ordered load complete: {successful_jobs}/{total_jobs} jobs successful")
        
        if results["errors"]:
            self.logger.warning(f"Encountered {len(results['errors'])} errors during loading")
            for error in results["errors"]:
                self.logger.warning(f"  - {error}")
        
        return results
