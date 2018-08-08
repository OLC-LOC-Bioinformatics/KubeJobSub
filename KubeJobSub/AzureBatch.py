#!/usr/bin/env python

# Core python library
import argparse
import os

# Azure-related imports - need blob service and batch service modules
from azure.batch import batch_service_client, batch_auth
from azure.storage.blob import BlockBlobService
import azure.batch.models as batchmodels


# To be put into a configuration file:
BATCH_ACCOUNT_NAME = ''
BATCH_ACCOUNT_KEY = ''
BATCH_ACCOUNT_URL = ''
STORAGE_ACCOUNT_NAME = ''
STORAGE_ACCOUNT_KEY = ''


def upload(blob_service, item_to_upload):
    # TODO: Make a recursive blob upload work.
    if os.path.isfile(item_to_upload):
        blob_service.create_blob_from_path(container_name=args.name,
                                           blob_name=os.path.split(item_to_upload)[-1],
                                           file_path=item_to_upload)


def download_container(blob_service, container_name, output_dir):
    # TODO: Test that this works on multi-nested stuff.
    # Modified from https://blogs.msdn.microsoft.com/brijrajsingh/2017/05/27/downloading-a-azure-blob-storage-container-python/
    generator = blob_service.list_blobs(container_name)
    for blob in generator:
        # check if the path contains a folder structure, create the folder structure
        if "/" in blob.name:
            # extract the folder path and check if that folder exists locally, and if not create it
            head, tail = os.path.split(blob.name)
            if os.path.isdir(os.path.join(output_dir, head)):
                # download the files to this directory
                blob_service.get_blob_to_path(container_name, blob.name, os.path.join(output_dir, head, tail))
            else:
                # create the diretcory and download the file to it
                os.makedirs(os.path.join(output_dir, head))
                blob_service.get_blob_to_path(container_name, blob.name, os.path.join(output_dir, head, tail))
        else:
            blob_service.get_blob_to_path(container_name, blob.name, blob.name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--command',
                        type=str,
                        required=True,
                        help='Command you would run on your own system. Put it in double quotes.')
    parser.add_argument('-r', '--results',
                        type=str,
                        required=True,
                        help='Folder or file you want to be uploaded to blob storage as result.')
    parser.add_argument('-i', '--input',
                        type=str,
                        nargs='+',
                        required=True,
                        help='Folder or file you want to upload to blob storage to use as input.')
    parser.add_argument('-n', '--name',
                        type=str,
                        required=True,
                        help='Name for your job. Must be unique. Output goes to name-output.')
    parser.add_argument('-d', '--conda',
                        type=str,
                        required=True,
                        help='Conda package to install.')
    args = parser.parse_args()

    # Get block blob service going.
    blob_service = BlockBlobService(account_key=STORAGE_ACCOUNT_KEY,
                                    account_name=STORAGE_ACCOUNT_NAME)

