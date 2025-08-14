"""
Neptune Loader using AWS SDK instead of direct HTTPS calls
This fixes the 403 Forbidden error by using proper AWS authentication
"""

import logging
import time
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from urllib.parse import urlparse

class NeptuneLoaderSDK:
    def __init__(self, neptune_endpoint, iam_role_arn=None, region_name="us-east-1"):
        """
        Initialize Neptune loader using AWS SDK
        
        Args:
            neptune_endpoint: Neptune endpoint URL (without https:// and port)
            iam_role_arn: IAM role ARN for Neptune to access S3
            region_name: AWS region name
        """
        self.neptune_endpoint = neptune_endpoint
        self.iam_role_arn = iam_role_arn
        self.region_name = region_name
        self.logger = logging.getLogger(__name__)
        
        # Clean up the endpoint - remove https:// and port if present
        if self.neptune_endpoint.startswith("https://"):
            self.neptune_endpoint = self.neptune_endpoint[8:]  # Remove https://
        if self.neptune_endpoint.startswith("http://"):
            self.neptune_endpoint = self.neptune_endpoint[7:]   # Remove http://
        if ":8182" in self.neptune_endpoint:
            self.neptune_endpoint = self.neptune_endpoint.replace(":8182", "")
        
        # Construct the full endpoint URL for neptunedata client
        self.endpoint_url = f"https://{self.neptune_endpoint}:8182"
        
        # Initialize Neptune DATA client (not management client)
        try:
            self.neptune_client = boto3.client(
                'neptunedata',
                region_name=self.region_name,
                endpoint_url=self.endpoint_url
            )
            self.logger.info(f"Initialized Neptune DATA client for endpoint: {self.endpoint_url}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Neptune DATA client: {e}")
            raise
    
    def start_load_job(self, s3_uri, load_format="csv", fail_on_error=False, 
                       parallelism="MEDIUM", mode="AUTO"):
        """
        Start a Neptune loader job using AWS SDK
        
        Args:
            s3_uri: S3 URI of the data to load
            load_format: Format of the data (csv, opencypher)
            fail_on_error: Whether to fail on error
            parallelism: Parallelism level (LOW, MEDIUM, HIGH, OVERSUBSCRIBE)
            mode: Load mode (AUTO, RESUME, NEW)
            
        Returns:
            Load job ID if successful, None otherwise
        """
        try:
            # Extract S3 bucket region from the URI
            s3_bucket_region = self._get_s3_bucket_region(s3_uri)
            
            self.logger.info(f"Starting Neptune load job for: {s3_uri}")
            self.logger.info(f"Parameters: format={load_format}, failOnError={fail_on_error}, parallelism={parallelism}")
            self.logger.info(f"S3 bucket region: {s3_bucket_region}, IAM role: {self.iam_role_arn}")
            
            response = self.neptune_client.start_loader_job(
                source=s3_uri,
                format=load_format,
                s3BucketRegion=s3_bucket_region,
                iamRoleArn=self.iam_role_arn,
                mode=mode,
                failOnError=fail_on_error,
                parallelism=parallelism,
                updateSingleCardinalityProperties=False,
            )
            
            load_id = response.get('payload', {}).get('loadId')
            self.logger.info(f"✅ Started Neptune load job {load_id} for {s3_uri}")
            return load_id
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            self.logger.error(f"❌ AWS ClientError starting Neptune load job: {error_code} - {error_message}")
            self.logger.error(f"   S3 URI: {s3_uri}")
            self.logger.error(f"   IAM Role: {self.iam_role_arn}")
            self.logger.error(f"   Endpoint: {self.endpoint_url}")
            return None
        except Exception as e:
            self.logger.error(f"❌ Error starting Neptune load job: {e}")
            self.logger.error(f"   S3 URI: {s3_uri}")
            self.logger.error(f"   Client type: {type(self.neptune_client)}")
            return None
    
    def get_load_status(self, load_id):
        """
        Get the status of a Neptune loader job using AWS SDK
        
        Args:
            load_id: Load job ID
            
        Returns:
            Status dictionary if successful, None otherwise
        """
        try:
            response = self.neptune_client.get_loader_job_status(loadId=load_id)
            payload = response.get('payload', {})
            
            overall_status = payload.get('overallStatus', {})
            status = overall_status.get('status', 'UNKNOWN')
            
            self.logger.debug(f"Load job {load_id} status: {status}")
            return payload
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            self.logger.error(f"❌ AWS ClientError getting load job status: {error_code} - {error_message}")
            return None
        except Exception as e:
            self.logger.error(f"❌ Error getting load job status: {e}")
            return None
    
    def wait_for_load_completion(self, load_id, poll_interval=15, timeout=1800):
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
        self.logger.info(f"Waiting for load job {load_id} to complete (timeout: {timeout}s)...")
        
        while time.time() - start_time < timeout:
            status = self.get_load_status(load_id)
            if not status:
                return None
            
            overall_status = status.get("overallStatus", {}).get("status")
            
            if overall_status in ["LOAD_COMPLETED", "LOAD_COMPLETED_WITH_ERRORS"]:
                elapsed = int(time.time() - start_time)
                self.logger.info(f"✅ Load job {load_id} completed with status {overall_status} (took {elapsed}s)")
                return status
            
            if overall_status in ["LOAD_FAILED", "LOAD_CANCELLED_BY_USER"]:
                self.logger.error(f"❌ Load job {load_id} failed with status {overall_status}")
                # Log error details if available
                if 'errorDetails' in status:
                    for error in status['errorDetails'][:3]:  # Show first 3 errors
                        self.logger.error(f"   Error: {error}")
                return status
            
            # Log progress for long-running jobs
            elapsed = int(time.time() - start_time)
            if elapsed % 60 == 0:  # Every minute
                self.logger.info(f"Load job {load_id} still running ({overall_status}) - elapsed: {elapsed}s")
            
            time.sleep(poll_interval)
        
        self.logger.error(f"❌ Timeout waiting for load job {load_id} to complete")
        return None
    
    def _get_s3_bucket_region(self, s3_uri):
        """
        Get the region of an S3 bucket from the URI
        
        Args:
            s3_uri: S3 URI (e.g., s3://bucket/key)
            
        Returns:
            S3 bucket region
        """
        try:
            # Parse S3 URI to get bucket name
            if not s3_uri.startswith('s3://'):
                raise ValueError(f"Invalid S3 URI: {s3_uri}")
            
            bucket_name = s3_uri[5:].split('/')[0]  # Remove s3:// and get bucket name
            
            # Get bucket region
            s3_client = boto3.client('s3')
            response = s3_client.get_bucket_location(Bucket=bucket_name)
            region = response.get('LocationConstraint')
            
            # Handle special case for us-east-1
            if region is None:
                region = 'us-east-1'
            
            self.logger.debug(f"S3 bucket {bucket_name} is in region: {region}")
            return region
            
        except Exception as e:
            self.logger.warning(f"Could not determine S3 bucket region, using default: {self.region_name}")
            return self.region_name
    
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
                bucket = s3_path.rstrip('/')
                prefix = ""
            else:
                parts = s3_path.split('/', 1)
                bucket = parts[0]
                prefix = parts[1].rstrip('/')
                if prefix:
                    prefix += '/'
            
            self.logger.info(f"Listing files in S3 bucket: {bucket}, prefix: {prefix}")
            
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
            
            self.logger.info(f"Found {len(files)} files in {s3_uri}")
            return files
            
        except Exception as e:
            self.logger.error(f"Error listing S3 files: {e}")
            return []
    
    def start_ordered_load_job(self, s3_uri, load_format="csv", fail_on_error=False,
                              parallelism="MEDIUM", mode="AUTO", 
                              poll_interval=15, timeout=1800):
        """
        Start Neptune loader jobs with proper ordering: nodes first, then edges
        
        Args:
            s3_uri: S3 URI of the directory containing CSV files
            load_format: Format of the data (csv, opencypher)
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
            return {"status": "failed", "error": "No files found", "node_jobs": [], "edge_jobs": [], "errors": []}
        
        # Separate node and edge files
        node_files = [f for f in all_files if '/node_' in f and f.endswith('.csv')]
        edge_files = [f for f in all_files if '/edges_' in f and f.endswith('.csv')]
        
        self.logger.info(f"Found {len(node_files)} node files and {len(edge_files)} edge files")
        
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
                    node_file, load_format, fail_on_error, parallelism, mode
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
                    edge_file, load_format, fail_on_error, parallelism, mode
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
        
        self.logger.info(f"Ordered Neptune load completed: {successful_jobs}/{total_jobs} jobs successful")
        
        if results["errors"]:
            self.logger.warning(f"Encountered {len(results['errors'])} errors during load")
        
        return results
