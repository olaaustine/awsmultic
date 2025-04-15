Uploading and copying files in S3 is fast only if the file is less than 5GB, this is a python project to speed up the process. 

Using this module speeds up the process from more than 20 minutes to 30 seconds 

Documentation - [here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/CopyingObjectsMPUapi.html)

To copy/upload files from local to a remote S3 bucket: 

## Commands 
``` python3 -m bin.upload --bucket awsmulticopy --newf new_folder --file data/census-data.bin ```

Ensure your `~/aws/credentials` has the right keys for this bucket 

