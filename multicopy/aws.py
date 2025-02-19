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
    def check_permission_for_bucket(self, bucket):
        """
            Check if "user" has permission for the bucket using S3 get-bucket-acl

            Args:
                bucket (_type_): Bucket name in S3 to check if user has permission for this bucket 
                Ex of bucket - multicopy
        """
        logging.info("Checking permissions for the S3 bucket")

        command = ["aws", "s3api", "get-bucket-acl", "--bucket", bucket]

        try:
            result = subprocess.run(command, check=True, capture_output=True)
            logging.info(f"✅User has permission for this bucket {str(result.stdout)}")
        except ProcessLookupError:
            logging.error("User does not have permission for this bucket")

    def check_size_of_file(self, file):
        """
            Check sizes of file to be moved/copied or uploaded 

            Args:
                file (_type_): file
            Returns:
                _type_: the size of the file in GB
        """        
        logging.info("Checking the size of the files to determine upload style")
        size = os.path.getsize(file)
        size = size * 1024 * 1024 * 1024

        return size

    def simple_copy_delete(self, file, current, new, bucket):
        """
            Simple, copy and delete function if size is less than 5GB

            Args:
                file (_type_): file to copy 
                current (_type_): bucket folder where the file is 
                new (_type_): bucket folder where the file needs to move to 
                bucket (_type_): the S3 bucket name 

            Returns:
                _type_: True if complete, return None if not 
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
                        f"✅ File in the old path has been removed, {result_rm.stdout}"
                    )
                    return True
                except subprocess.CalledProcessError as e:
                    logging.error(f"❌ File could not be removed. {str(e)}")
                    return None
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ File could not be copied, {str(e)}")
            return None

    def create_copy_part(self, file, new, bucket):
        """
            Create a copy part, the first step in the UploadCopyPart for AWS S3
            Args:
                file (_type_): file to copy
                new (_type_): the new folder to copy the file
                bucket (_type_): the bucket in which the folders

            Returns:
                _type_: UploadId
        """        
        if self.check_size_of_file > 5:
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
                logging.error(f"❌ Could not create a multipart upload {str(e)}")
                return None

    def upload_copy_parts(
        self, file, new_file, uploadid, bucket, new, chunk_size=500 * 1024 * 1024
    ):
        """
            Upload/Copy file using parts and S3 boto client, upload part 

            Args:
                file (_type_): file you want to move
                new_file (_type_): the new file we are copying to, incase we want to rename 
                uploadid (_type_): From the response object when you create a copy part
                bucket (_type_): Bucket where the folders are 
                new (_type_): new folder
                chunk_size (_type_, optional): _description_. Defaults to 500*1024*1024.
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
                            Key=f"{new}/{new_file}",  # Ensure correct key format
                            PartNumber=part_number,
                            UploadId=uploadid,
                            Body=chunk_data,
                            ChecksumSHA256=sha256_hash,
                        )
                        logging.info(
                            f"✅ Uploaded part {part_number}: {chunk_name} - ETag: {response['ETag']}"
                        )
                        part_number += 1  # Increment after successful upload

                    except Exception as e:
                        logging.error(
                            f"❌ Failed to upload part {chunk_name} - {str(e)}"
                        )
                        break  # Stop the loop on error (optional)

        except FileNotFoundError as e:
            logging.error(f"❌ Could not open file: {str(e)}")
        except Exception as e:
            logging.error(f"❌ Unexpected error: {str(e)}")

    def list_uploaded_parts(self, file, uploadid, bucket, new):
        """
            Listing uploaded parts which will be used to complete multipart upload

            Args:
                file (_type_): file we are moving/uploading or copying
                uploadid (_type_): From the response object when you create a copy part
                bucket (_type_): S3 bucket
                new (_type_): new folder

            Returns:
                _type_: A Dictionary containing the JSON object from the AWS S3 response
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
            logging.error(f"❌ Failed to list parts: {str(e)}")
            return None

    def complete_multipart_upload(self, bucket, new, file, uploadid, parts_json):
        """
            The last step of the AWS S3 Multipart process.

            Args:
                bucket (_type_): S3 bucket
                new (_type_): New folder
                file (_type_): file
                uploadid (_type_): UploadID of the object
                parts_json (_type_): Uploaded part JSON

            Returns:
                _type_: AWS S3 response or None if it fails
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
            logging.info(f"✅ Uploaded file {result.stdout}")
            return result.stdout  # Return AWS response
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ Failed to complete multipart upload: {str(e)}")
            return None
