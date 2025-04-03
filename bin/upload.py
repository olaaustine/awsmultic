import click 
import logging
import sys
import os

from multicopy.aws import AWS

aws = AWS()
@click.command()
@click.option('--bucket', '-b', required=True, help="Bucket name")
@click.option('--newf', '-nf', required=True, help="Folder to move the object to in the bucket")
@click.option('--file', '-f', required=True, help="Present file in the local space or directory")
# @click.option('--new_file', '-nfile', required=True, help="New file name")
@click.option('--chunk_size', '-cs', required=False, type=int, help="Chunk size for multipart upload, the default is 500MB")

def main(bucket: str, newf: str, file: str, chunk_size: int) -> None:
    """
    Handles the multipart copy process for an S3 bucket.

    Args:
        bucket (str): Bucket to upload
        newf (str): New folder to upload in the bucket
        file (str): File to upload
        chunk_size (int): optional 
        
    """    

    if aws.check_permission_for_bucket(bucket):
        id = aws.create_copy_part(file, newf, bucket)

        if id:
            aws.upload_copy_parts(file, id, bucket, newf)
            parts_json = aws.list_uploaded_parts(file, id, bucket, newf)

            if parts_json:
                aws.complete_multipart_upload(bucket, newf, file, id, parts_json)
                click.echo(f"✅{file} has been uploaded!" )

if __name__ == "__main__":
    main()

                   




