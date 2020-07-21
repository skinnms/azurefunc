import logging
import azure.functions as func
import paramiko
import os
import boto3
from botocore.exceptions import NoCredentialsError
import datetime

# sftp info
hostname = os.environ["hostname"]
username = os.environ["uid"]
password = os.environ["pwd"]
directory = os.environ["dir"]


def get_data():
    # create transport and sftp client
    transport = paramiko.Transport((hostname, 22))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # get the lastest file from the correct directory
    sftp.chdir(directory)
    latest = 0
    latestfile = None
    data = "file.json"
    for fileattr in sftp.listdir_attr():
        if fileattr.filename.startswith("LP") and fileattr.st_mtime > latest:
            latest = fileattr.st_mtime
            latestfile = fileattr.filename
    if latestfile is not None:
        sftp.get(latestfile, data)
    return data


# s3 info
datetime = datetime.datetime.now()
access_key = os.environ["ACCESS_KEY"]
secret_key = os.environ["SECRET_KEY"]
bucket_name = os.environ["bucket_name"]
s3_file_name = f"upload/data-{datetime}.json"


def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client(
        "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
    )

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


# function to remove the file once loaded
def remove_file():

    # create transport and sftp client
    transport = paramiko.Transport((hostname, 22))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # get the lastest file from the correct directory and remove it
    sftp.chdir(directory)
    latest = 0
    latestfile = None
    for fileattr in sftp.listdir_attr():
        if fileattr.filename.startswith("LP") and fileattr.st_mtime > latest:
            latest = fileattr.st_mtime
            latestfile = fileattr.filename
    if latestfile is not None:
        sftp.remove(latestfile)
    return


# azure function http trigger and blob output binding
def main(req: func.HttpRequest):
    logging.info("Python HTTP trigger function processed a request.")

    try:
        # get the lastest file from sftp
        local_file = get_data()

        # Store output data using S3
        uploaded = upload_to_aws(local_file, bucket_name, s3_file_name)
        logging.info("Done transferring file from SFTP")

        # remove remote file
        remove_file()
        logging.info("Done removing file from SFTP")

    except Exception as e:
        logging.error("Error:")
        logging.error(e)
