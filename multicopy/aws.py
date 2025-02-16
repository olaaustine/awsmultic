import logging 
import boto3
import subprocess
import os
import json
import hashlib
import io
import glob
import tempfile
import base64


s3_client = boto3.client("s3")

logging.basicConfig(level=logging.INFO)

class AWS():
    def check_permission_for_bucket(self, bucket):
        logging.info("Checking permissions for the S3 bucket")

        command = ['aws', 's3api', 'get-bucket-acl', '--bucket', bucket]
        
        try:
            result = subprocess.run(command, check=True, capture_output=True)
            logging.info("User has permission for this bucket", str(result.stdou))
        except ProcessLookupError:
            logging.error("User does not have permission for this bucket")

    def check_size_of_file(self, file): 
        logging.info("Checking the size of the files to determine upload style")
        size = os.path.getsize(file)

        return size 


    def simple_copy_delete(self, file, current, new, bucket):
        logging.info("File size is below 5GB, so we are moving it the simple way")

        command = [ "aws", "s3", "cp", f"s3://{bucket}/{current}/{file}", f"s3://{bucket}/{new}/{file}"]

        try:
            result = subprocess.run(command, check=True, capture_output=True)
            logging.info("File has been copied to new path")
            if result.stdout:
                rm_coomand = ["aws", "s3", "rm", f"s3://{bucket}/{current}/{file}"]
                try: 
                    result_rm = subprocess.run(command, check=True, capture_output=True)
                    logging.info("File in the old path has been removed")
                except subprocess.CalledProcessError as e:
                    logging.error("File could not be removed.", str(e))
                    return None
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ File could not be copied, {str(e)}")
            return None


    def create_copy_part(self, file, new, bucket):
        logging.info("File size is above 5GB, so we have to do a multipart upload")

        command = [ "aws", "s3api", "create-multipart-upload", '--bucket', bucket, '--key', f"{new}/{file}", '--checksum-algorithm', 'sha256']

        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            print(result.stdout)
            if result.stdout:
                response = json.loads(result.stdout)
                return response.get('UploadId')
            else: 
                logging.error(f"❌ Could not create a multipart upload")
                return None
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ Could not create a multipart upload {str(e)}")
            return None
        
    def chunking_parts(self, file_path, chunk_size=500*1024*1024):
        chunked_files = []  # Store (chunk_name, chunk_data)
        
        try:
            with open(file_path, "rb") as file:
                part_number = 1
                while True:
                    chunk_data = file.read(chunk_size)  # Read file in chunks
                    if not chunk_data:
                        break  # Stop if no more data
                    
                    chunk_name = f"chunk_{part_number:03d}"  # e.g., "chunk_001"
                    chunked_files.append((chunk_name, io.BytesIO(chunk_data)))  # Store in memory
                    part_number += 1
        
        except FileNotFoundError as e:
            logging.error(f"❌ Could not chunk file: {str(e)}")
            return None
    
        return chunked_files       # Return list of (chunk_name, chunk_data)

    def upload_copy_parts(self, file, uploadid, bucket, new, chunked_files): 
        for i, (chunk_name, chunk_data) in enumerate(chunked_files, start=1):
             # Compute SHA256 checksum
            hasher = hashlib.sha256()
            hasher.update(chunk_data.getvalue())  # Read the chunk's bytes
            sha256_hash = base64.b64encode(hasher.digest()).decode("utf-8")  # Get checksum in hex format
            try:
                response = s3_client.upload_part(
                    Bucket=bucket,
                    Key=f"{new}/{file}",
                    PartNumber=i,
                    UploadId=uploadid,
                    Body=chunk_data.getvalue(),
                    ChecksumSHA256=sha256_hash # Directly upload from memory
                )
                logging.info(f"✅ Uploaded part {i}: {chunk_name} - ETag: {response['ETag']}")

            except Exception as e:
                logging.error(f"❌ Failed to upload part {i}: {chunk_name} - {str(e)}")

    def list_uploaded_parts(self, file, uploadid, bucket, new):
        command = [
            "aws", "s3api", "list-parts",
            "--bucket", bucket,
            "--key", f"{new}/{file}",
            "--upload-id", uploadid,
            "--query", 'Parts[*].{PartNumber: PartNumber, ETag: ETag, ChecksumSHA256: ChecksumSHA256}',
            "--output", "json"
        ]

        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            parts_json = json.loads(result.stdout)  # Convert output to JSON
            logging.info(f"✅ Uploaded parts have been listed")
            return {"Parts": parts_json}  
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ Failed to list parts: {str(e)}")
            return None


    def complete_multipart_upload(self, bucket, new, file, uploadid, parts_json):
        # using tmp file to create a temporary file to use to complete multipart upload
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as tmp_file:
            json.dump(parts_json, tmp_file)
            tmp_file_path = tmp_file.name
            logging.info(f"✅ Temp file has been created containing temporary file info")

        command = [
            "aws", "s3api", "complete-multipart-upload",
            "--multipart-upload", f"file://{tmp_file_path}",
            "--bucket", bucket,
            "--key", f"{new}/{file}",
            "--upload-id", uploadid
        ]

        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            logging.info(f"✅ Uploaded file {result.stdout}")
            return result.stdout  # Return AWS response
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ Failed to complete multipart upload: {str(e)}")
            return None




if __name__ == '__main__':
    bucket =  "awsmulticopy"
    local_file = "/Users/olaaustine/Documents/awsmultic/data/census-data.bin"
    file = "census-data.bin"
    new = "test-code"
    current = "new-folder"
    aws_class = AWS()
    chunk_parts = aws_class.chunking_parts(local_file)
    create_copy = aws_class.create_copy_part(file, new, bucket)
    upload_copy_parts = aws_class.upload_copy_parts(file, create_copy, bucket, new, chunk_parts)
    listed_uploaded = aws_class.list_uploaded_parts(file,create_copy,bucket,new)
    complete = aws_class.complete_multipart_upload(bucket, new, file, create_copy, listed_uploaded)
