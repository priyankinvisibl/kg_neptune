"""
Utility for initiating Neptune loader jobs
"""

import logging
import requests
import json
import time
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
