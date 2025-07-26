"""
Utility for uploading files to S3
"""

import os
import logging
import boto3
from botocore.exceptions import ClientError
from pathlib import Path

class S3Uploader:
    def __init__(self, bucket_name, region_name="us-east-1"):
        """
        Initialize S3 uploader
        
        Args:
            bucket_name: S3 bucket name
            region_name: AWS region name
        """
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.logger = logging.getLogger(__name__)
    
    def upload_file(self, file_path, s3_key=None):
        """
        Upload a file to S3
        
        Args:
            file_path: Path to the file to upload
            s3_key: S3 key to use (defaults to file name)
            
        Returns:
            S3 URI if successful, None otherwise
        """
        if s3_key is None:
            s3_key = os.path.basename(file_path)
        
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            s3_uri = f"s3://{self.bucket_name}/{s3_key}"
            self.logger.info(f"Uploaded {file_path} to {s3_uri}")
            return s3_uri
        except ClientError as e:
            self.logger.error(f"Error uploading {file_path} to S3: {e}")
            return None
    
    def upload_directory(self, directory_path, s3_prefix=""):
        """
        Upload all files in a directory to S3
        
        Args:
            directory_path: Path to the directory to upload
            s3_prefix: Prefix to add to S3 keys
            
        Returns:
            List of S3 URIs for uploaded files
        """
        directory_path = Path(directory_path)
        if not directory_path.is_dir():
            self.logger.error(f"{directory_path} is not a directory")
            return []
        
        s3_uris = []
        for file_path in directory_path.glob("**/*"):
            if file_path.is_file():
                relative_path = file_path.relative_to(directory_path)
                s3_key = f"{s3_prefix}/{relative_path}" if s3_prefix else str(relative_path)
                s3_uri = self.upload_file(str(file_path), s3_key)
                if s3_uri:
                    s3_uris.append(s3_uri)
        
        return s3_uris
