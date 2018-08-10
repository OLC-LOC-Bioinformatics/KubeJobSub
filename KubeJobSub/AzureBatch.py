#!/usr/bin/env python

# Core python library
import argparse
import datetime
import glob
import os

# Azure-related imports - need blob service and batch service modules
from azure.storage.blob import BlockBlobService
from azure.storage.blob import BlobPermissions
import azure.batch.batch_service_client as batch
import azure.batch.batch_auth as batch_auth
import azure.batch.models as batchmodels


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


class AzureBatch:
    def __init__(self):
        # Initially, have all these attrs set to None, and set them as needed.
        self.batch_account_name = None
        self.batch_account_key = None
        self.batch_account_url = None
        self.storage_account_name = None
        self.storage_account_key = None
        self.job_name = None
        self.command = None
        self.vm_image = None
        self.vm_size = 'Standard_D16s_v3'  # This should be sufficient for essentially anything.
        # Both input and output will be nested lists.
        # Explain this to your future self better soon.
        self.input = list()
        self.output = list()
        self.input_destination_dict = dict()

    def _login_to_batch(self):
        credentials = batch_auth.SharedKeyCredentials(self.batch_account_name, self.batch_account_key)
        batch_client = batch.BatchServiceClient(credentials, base_url=self.batch_account_url)
        return batch_client

    @staticmethod
    def _create_resource_file(blob_service, file_to_upload, input_container_name, destination_dir=False):
        blob_name = os.path.basename(file_to_upload)
        blob_service.create_blob_from_path(container_name=input_container_name,
                                           blob_name=blob_name,
                                           file_path=file_to_upload)
        sas_token = blob_service.generate_container_shared_access_signature(container_name=input_container_name,
                                                                            permission=BlobPermissions.READ,
                                                                            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))
        sas_url = blob_service.make_blob_url(container_name=input_container_name,
                                             blob_name=blob_name,
                                             sas_token=sas_token)
        if destination_dir:
            # This dictionary should be completely unnecessary now.
            # self.input_destination_dict[os.path.split(file_to_upload)[-1]] = destination_dir
            return batchmodels.ResourceFile(file_path=os.path.join(destination_dir, os.path.split(file_to_upload)[-1]),
                                            blob_source=sas_url)
        else:
            return batchmodels.ResourceFile(file_path=os.path.split(file_to_upload)[-1],
                                            blob_source=sas_url)

    def upload_input_to_blob_storage(self):
        # Instantiate our blob service! Maybe better to only do this once?
        resource_files = list()
        blob_service = BlockBlobService(account_key=self.storage_account_key,
                                        account_name=self.storage_account_name)
        # Create a container for input files - should be jobname-input, all lower case.
        # TODO: Add a check that this will be a valid container name - probably way upstream of this
        input_container_name = self.job_name.lower() + '-input'
        blob_service.create_container(container_name=input_container_name)
        for input_request in self.input:
            # If input request is only one item, just upload that to default dir on cloud
            if len(input_request.split()) == 1:
                files_to_upload = glob.glob(input_request)
                for file_to_upload in files_to_upload:
                    resource_files.append(self._create_resource_file(blob_service=blob_service,
                                                                     file_to_upload=file_to_upload,
                                                                     input_container_name=input_container_name))

            # If more than one item, last item is the destination directory on cloud vm that will run analysis.
            if len(input_request.split()) > 1:
                things_to_upload = input_request.split()
                destination_dir = things_to_upload.pop()
                for thing in things_to_upload:
                    files_to_upload = glob.glob(thing)
                    for file_to_upload in files_to_upload:
                        resource_files.append(self._create_resource_file(blob_service=blob_service,
                                                                         file_to_upload=file_to_upload,
                                                                         input_container_name=input_container_name,
                                                                         destination_dir=destination_dir))
                        # blob_service.create_blob_from_path(container_name=input_container_name,
                        #                                    blob_name=os.path.split(file_to_upload)[-1],
                        #                                    file_path=file_to_upload)
                        # self.input_destination_dict[os.path.split(file_to_upload)[-1]] = destination_dir
        return resource_files

    def create_pool(self):
        batch_client = self._login_to_batch()
        new_pool = batch.models.PoolAddParameter(
            id=self.job_name,
            virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
                image_reference=batchmodels.ImageReference(
                    publisher="Canonical",
                    offer="UbuntuServer",
                    sku="16.04-LTS",
                    version="latest"
                    # TODO: Change this once this is figured out.
                    # virtual_machine_image_id=self.vm_image,
                    ),
                node_agent_sku_id="batch.node.ubuntu 16.04"),
            vm_size=self.vm_size,
            target_dedicated_nodes=1,
            target_low_priority_nodes=0,
        )
        batch_client.pool.add(new_pool)

    def create_job(self):
        batch_client = self._login_to_batch()
        job = batch.models.JobAddParameter(self.job_name, batch.models.PoolInformation(pool_id=self.job_name))
        batch_client.job.add(job)

    def prepare_output_resource_files(self):
        # TODO
        output_resource_files = list()
        print(self.output)
        return output_resource_files

    def create_task(self, input_files):
        blob_service = BlockBlobService(account_key=self.storage_account_key,
                                        account_name=self.storage_account_name)
        # Need an output container created.
        output_container_name = self.job_name.lower() + '-output'
        blob_service.create_container(container_name=output_container_name)
        sas_token = blob_service.generate_container_shared_access_signature(container_name=output_container_name,
                                                                            permission=BlobPermissions.WRITE,
                                                                            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2))
        sas_url = 'https://{}.blob.core.windows.net/{}?{}'.format(self.storage_account_name, output_container_name, sas_token)
        # TODO: this will be gone too once we actually have things set up properly.
        confindr_database = batchmodels.ResourceFile(blob_source='https://carlingst01.blob.core.windows.net/databases/confindr.tar.gz',
                                                     file_path='confindr.tar.gz')
        input_files.append(confindr_database)
        batch_client = self._login_to_batch()
        # TODO: Change this up to actually use the user command.
        command = "/bin/bash -c \"wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh" \
                  " && bash Miniconda3-latest-Linux-x86_64.sh -b -p $HOME/miniconda && export PATH=\"$HOME/miniconda/bin:$PATH\" " \
                  "&& conda config --add channels bioconda && conda config --add channels conda-forge && " \
                  "conda install -c bioconda confindr && conda update bbmap && tar xf confindr.tar.gz && confindr.py -i input -o output -d databases\""
        task = batch.models.TaskAddParameter(
            id='Task1',
            command_line=command,
            resource_files=input_files,
            output_files=[batchmodels.OutputFile('output/confindr_report.csv',
                          destination=batchmodels.OutputFileDestination(
                            container=batchmodels.OutputFileBlobContainerDestination(sas_url)),
                            upload_options=batchmodels.OutputFileUploadOptions(
                            batchmodels.OutputFileUploadCondition.task_success))]
            )
        batch_client.task.add(job_id=self.job_name, task=task)


def parse_configuration_file(config_file):
    """
    Parse the configuration file a user provides and return an insantiated object that can do all the things - seems
    to most likely be the best way to do things.

    It seems that the best way to do this may, sadly, be to make users write a config file.
    Things we'll need in the config file:
    # Azure-related things - should be able to have these pre-filled for people.
    BATCH_ACCOUNT_NAME=''
    BATCH_ACCOUNT_KEY=''
    BATCH_ACCOUNT_URL=''
    STORAGE_ACCOUNT_NAME = ''
    STORAGE_ACCOUNT_KEY = ''
    # Not sure if we'll actually need job name. TBD.
    JOB_NAME =
    # Allow multiple input files - each one can be a unix-y mv, with the last arg being a folder to place the files
    in on cloud VM.
    INPUT =
    # Also allow multiple output files - each will get uploaded to blob storage, and optionally download from blob
    storage to user's computer.
    OUTPUT =
    # The command to run on cloud.
    COMMAND =
    # The URL for the VM image user wants to run - will need to have a list somewhere showing what VMs have what
    programs installed
    VM_IMAGE =
    # Have a default VM size that should be sufficient for essentially anything, but allow for custom VMs
    VM_SIZE =
    """
    with open(config_file) as f:
        config_options = f.readlines()

    azurebatch = AzureBatch()
    unrecognized_options = list()

    # Go through the input file and parse through all the things.
    # If user has specified any options that are not part of our set of recognized options, boot them out with a message
    for config_option in config_options:
        config_option = config_option.rstrip()
        x = config_option.split(':=')
        option = x[0]
        parameter = x[1]
        # Unfortunate if structure :(
        if option == 'BATCH_ACCOUNT_NAME':
            azurebatch.batch_account_name = parameter
        elif option == 'BATCH_ACCOUNT_KEY':
            azurebatch.batch_account_key = parameter
        elif option == 'BATCH_ACCOUNT_URL':
            azurebatch.batch_account_url = parameter
        elif option == 'STORAGE_ACCOUNT_NAME':
            azurebatch.storage_account_name = parameter
        elif option == 'STORAGE_ACCOUNT_KEY':
            azurebatch.storage_account_key = parameter
        elif option == 'JOB_NAME':
            azurebatch.job_name = parameter
        elif option == 'COMMAND':
            azurebatch.command = parameter
        elif option == 'INPUT':
            azurebatch.input.append(parameter)
        elif option == 'OUTPUT':
            azurebatch.output.append(parameter)
        elif option == 'VM_IMAGE':
            azurebatch.vm_image = parameter
        elif option == 'VM_SIZE':
            azurebatch.vm_size = parameter
        else:
            unrecognized_options.append(option)

    # Check that no options were submitted that were not recognized.
    if len(unrecognized_options) > 0:
        raise AttributeError('The following options were specified in configuration file {config_file},'
                             ' but not recognized: {options}'.format(options=unrecognized_options,
                                                                     config_file=config_file))

    return azurebatch


def check_no_attributes_none(azurebatch_object):
    missing_attributes = list()
    attrs = vars(azurebatch_object)
    for attr in attrs:
        if attrs[attr] is None:
            missing_attributes.append(attr.upper())
        elif type(attrs[attr]) is list:
            if len(attrs[attr]) == 0:
                missing_attributes.append(attr.upper())
    if len(missing_attributes) > 0:
        raise AttributeError('The following options are required, but were not found in your '
                             'configuration file: {}'.format(missing_attributes))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--configuration_file',
                        type=str,
                        required=True,
                        help='Path to your configuration file.')
    args = parser.parse_args()

    azurebatch = parse_configuration_file(args.configuration_file)
    check_no_attributes_none(azurebatch)
    resource_files = azurebatch.upload_input_to_blob_storage()
    azurebatch.create_pool()
    azurebatch.create_job()
    azurebatch.create_task(input_files=resource_files)
    # TODO: Add wait for task to complete and then clean up pool/job/some blobs?
