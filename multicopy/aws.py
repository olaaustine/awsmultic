import logging
import boto3
import subprocess
import os
import json
import hashlib
import tempfile
import base64


s3_client = boto3.client("s3")

logging.basicConfig(level=logging.INFO)


class AWS:
    def check_permission_for_bucket(self, bucket: str) -> bool:
        """
            Check if "user" has permission for the bucket using S3 get-bucket-acl

            Args:
                bucket (str): Bucket name in S3 to check if user has permission for this bucket 
                Ex of bucket - multicopy
            Returns:
            bool: True or False if user has permissions
        """

        logging.info("Checking permissions for the S3 bucket")

        command = ["aws", "s3api", "get-bucket-acl", "--bucket", bucket]

        try:
            result = subprocess.run(command, check=True, capture_output=True)
            logging.info("✅User has permission for this bucket %s", str(result.stdout))
            return True
        except ProcessLookupError:
            logging.error("User does not have permission for this bucket")
            return False

    def check_size_of_file(self, file: str)-> int:
        """
            Check sizes of file to be moved/copied or uploaded 

            Args:
                file (str): File to be checked

            Returns:
                int: size of the file
        """             
        logging.info("Checking the size of the files to determine upload style")
        size = os.path.getsize(file)
        size = size * 1024 * 1024 * 1024

        return size

    def simple_copy_delete(self, file: str, current: str, new: str, bucket: str) -> bool:
        """
            Simple, copy and delete function if size is less than 5GB

            Args:
                file (str): file to copy
                current (str): bucket folder where the file is 
                new (str): bucket folder where the file needs to move to 
                bucket (str): the S3 bucket name 

            Returns:
                bool: True if complete, return False if not 
        """      
        logging.info("File size is below 5GB, so we are moving it the simple way")

        command = [
            "aws",
            "s3",
            "cp",
            f"s3://{bucket}/{current}/{file}",
            f"s3://{bucket}/{new}/{file}",
        ]

        try:
            result = subprocess.run(command, check=True, capture_output=True)
            logging.info("File has been copied to new path")
            if result.stdout:
                rm_command = ["aws", "s3", "rm", f"s3://{bucket}/{current}/{file}"]
                try:
                    result_rm = subprocess.run(
                        rm_command, check=True, capture_output=True
                    )
                    logging.info(
                        "✅ File in the old path has been removed %s", result_rm.stdout)
                    return True
                except subprocess.CalledProcessError as e:
                    logging.error("❌ File could not be removed. %s", str(e))
                    return False
        except subprocess.CalledProcessError as e:
            logging.error("❌ File could not be copied %s", str(e))
            return False

    def create_copy_part(self, file: str, new: str, bucket: str) -> str:
        """
            Create a copy part, the first step in the UploadCopyPart for AWS S3

            Args:
                file (str): The file to be copied
                new (str): New folder to copy the file
                bucket (str): Bucket name

            Returns:
                str: AWS upload ID 
        """
        word = True    
        if word:
            logging.info("File size is above 5GB, so we have to do a multipart upload")

            command = [
                "aws",
                "s3api",
                "create-multipart-upload",
                "--bucket",
                bucket,
                "--key",
                f"{new}/{file}",
                "--checksum-algorithm",
                "sha256",
            ]

            try:
                result = subprocess.run(
                    command, check=True, capture_output=True, text=True
                )
                print(result.stdout)
                if result.stdout:
                    response = json.loads(result.stdout)
                    return response.get("UploadId")
                else:
                    logging.error("❌ Could not create a multipart upload")
                    return None
            except subprocess.CalledProcessError as e:
                logging.error("❌ Could not create a multipart upload %s", str(e))
                return None

    def upload_copy_parts(
        self, file: str, uploadid: str, bucket: str, new: str, chunk_size: int=5 * 1024 * 1024
    ) -> bool:
        """
            Upload/Copy file using parts and S3 boto client, upload part 

            Args:
                file (str): file you want to move
                uploadid (str): From the response object when you create a copy part
                bucket (str): Bucket where the folders are 
                new (str): new folder
                chunk_size (int, optional): Chunking size. Defaults to 500*1024*1024.

            Returns:
                bool: _description_
        """        
        try:
            with open(file, "rb") as opened_file:
                part_number = 1
                while True:
                    chunk_data = opened_file.read(chunk_size)
                    if not chunk_data:
                        break  # Exit if no more data to read

                    chunk_name = f"chunk_{part_number:03d}"
                    hasher = hashlib.sha256()
                    hasher.update(chunk_data)
                    sha256_hash = base64.b64encode(hasher.digest()).decode("utf-8")

                    try:
                        response = s3_client.upload_part(
                            Bucket=bucket,
                            Key=f"{new}/{file}",  # Ensure correct key format
                            PartNumber=part_number,
                            UploadId=uploadid,
                            Body=chunk_data,
                            ChecksumSHA256=sha256_hash,
                        )
                        logging.info("✅ Uploaded part %s: %s - ETag: %s", part_number, chunk_name, response["ETag"])
                        part_number += 1  # Increment after successful upload

                    except Exception as e:
                        logging.error(
                            "❌ Failed to upload part %s - %s", chunk_name, str(e)
                        )
                        break  # Stop the loop on error
        except FileNotFoundError as e:
            logging.error("❌ Could not open file: %s",str(e))
            return False
        except Exception as e:
            logging.error("❌ Unexpected error: %s", str(e))
            return False
        return True

    def list_uploaded_parts(self, file: str, uploadid: str, bucket: str, new: str) -> object:
        """
            Listing uploaded parts which will be used to complete multipart upload

            Args:
                file (str): file we are moving/uploading or copying
                uploadid (str): From the response object when you create a copy part
                bucket (str):  S3 bucket
                new (str): new folder

            Returns:
                object:  A Dictionary containing the JSON object from the AWS S3 response
        """        
        command = [
            "aws",
            "s3api",
            "list-parts",
            "--bucket",
            bucket,
            "--key",
            f"{new}/{file}",
            "--upload-id",
            uploadid,
            "--query",
            "Parts[*].{PartNumber: PartNumber, ETag: ETag, ChecksumSHA256: ChecksumSHA256}",
            "--output",
            "json",
        ]

        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            parts_json = json.loads(result.stdout)  # Convert output to JSON
            logging.info("✅ Uploaded parts have been listed")
            return {"Parts": parts_json}
        except subprocess.CalledProcessError as e:
            logging.error("❌ Failed to list parts: %s", str(e))
            return None

    def complete_multipart_upload(self, bucket: str, new: str, file: str, uploadid: str, parts_json: object) -> str :
        """
            The last step of the AWS S3 Multipart process.

            Args:
                bucket (str): S3 bucket
                new (str): New folder
                file (str): file
                uploadid (str): UploadID of the object
                parts_json (object): Uploaded part JSON

            Returns:
                str: AWS S3 response or None if it fails
        """        

        # using tmp file to create a temporary file to use to complete multipart upload
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".json"
        ) as tmp_file:
            json.dump(parts_json, tmp_file)
            tmp_file_path = tmp_file.name
            logging.info("✅ Temp file has been created containing temporary file info")

        command = [
            "aws",
            "s3api",
            "complete-multipart-upload",
            "--multipart-upload",
            f"file://{tmp_file_path}",
            "--bucket",
            bucket,
            "--key",
            f"{new}/{file}",
            "--upload-id",
            uploadid,
        ]

        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            logging.info("✅ Uploaded file %s", result.stdout)
            return result.stdout  # Return AWS response
        except subprocess.CalledProcessError as e:
            logging.error("❌ Failed to complete multipart upload: %s", str(e))
            return None
