import logging 
#import boto3
import subprocess
#import requests 
import os
import json
import glob
import tempfile


logging.basicConfig(level=logging.INFO)

class AWS():
    def check_permission_for_bucket(self, bucket):
        logging.info("Checking permissions for the S3 bucket")

        command = ['aws', 's3api', 'get-bucket-acl', '--bucket', bucket]
        
        try:
            result = subprocess.run(command, check=True, capture_output=True)
            logging.info("User has permission for this bucket", str(result.stdout))
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

    def chunking_parts(self, file, chunk_size="5M", output_prefix="chunk_" ):
        command = ["split", "-b", chunk_size, "-d", file, output_prefix]
        try:
            subprocess.run(command, check=True)
            chunked_files = sorted(glob.glob(f"{output_prefix}*"))  # Get sorted list of chunks
            return chunked_files
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ Could not chunk files, {str(e)}")
            return None

    
    def upload_copy_parts(self, file, uploadid, bucket, new, chunked_files):
        for i, chunk_file in enumerate(chunked_files, start=1):  # Start part numbers from 1
            command = [
                "aws", "s3api", "upload-part",
                "--bucket", bucket,
                "--key", f"{new}/{file}",  # Final object name
                "--part-number", str(i),
                "--body", chunk_file,
                "--upload-id", uploadid,
               '--checksum-algorithm', 'sha256'
            ]
            try:
                subprocess.run(command, check=True)
                logging.info(f"✅ Uploaded part {i}: {chunk_file}")
            except subprocess.CalledProcessError as e:
                logging.error(f"❌ Failed to upload part {i}: {chunk_file} - {str(e)}")
    
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



