# Apparently this is how to import things if they don't end in .py
# This means that pytest has to be run from root of project dir.
exec(open('KubeJobSub/AzureBatch').read())
import pytest
from azure.storage.blob import BlockBlobService


def test_bad_batch_credentials():
    azurebatch = AzureBatch()
    azurebatch.batch_account_name = 'bad_account_name'
    azurebatch.batch_account_key = 'badaccountkey'
    azurebatch.batch_account_url = 'bad_account_url'
    with pytest.raises(AttributeError):
        azurebatch._login_to_batch()


def test_good_batch_credentials():
    azurebatch = parse_configuration_file('tests/valid_credentials.txt')
    azurebatch._login_to_batch()


def test_bad_batch_url():
    azurebatch = parse_configuration_file('tests/valid_credentials.txt')
    azurebatch.batch_account_url = 'bad_account_url'
    with pytest.raises(AttributeError):
        azurebatch._login_to_batch()


def test_bad_batch_account_name():
    azurebatch = parse_configuration_file('tests/valid_credentials.txt')
    azurebatch.batch_account_name = 'badaccountname'
    with pytest.raises(AttributeError):
        azurebatch._login_to_batch()


def test_bad_batch_account_key():
    azurebatch = parse_configuration_file('tests/valid_credentials.txt')
    azurebatch.batch_account_key = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/GKA=='
    with pytest.raises(AttributeError):
        azurebatch._login_to_batch()


def test_job_create_on_nonexistent_pool():
    azurebatch = parse_configuration_file('tests/valid_credentials.txt')
    with pytest.raises(AttributeError):
        azurebatch.create_job()


def test_delete_nonexistent_pool():
    azurebatch = parse_configuration_file('tests/valid_credentials.txt')
    with pytest.raises(AttributeError):
        azurebatch.delete_pool()


def test_upload_single_input_file():
    azurebatch = parse_configuration_file('tests/valid_credentials.txt')
    azurebatch.input = {'': ['tests/valid_credentials.txt']}
    azurebatch.job_name = 'pytest-1'
    resource_files = azurebatch.upload_input_to_blob_storage(input_id='')
    blob_client = BlockBlobService(account_name=azurebatch.storage_account_name,
                                   account_key=azurebatch.storage_account_key)
    generator = blob_client.list_blobs(container_name=azurebatch.job_name + '-input')
    blob_files = list()
    for blob in generator:
        blob_files.append(blob.name)
    assert len(blob_files) == 1
    assert blob_files[0] == 'valid_credentials.txt'
    assert len(resource_files) == 1
    blob_client.delete_container(container_name=azurebatch.job_name + '-input')


def test_upload_single_file_to_dest_dir():
    azurebatch = parse_configuration_file('tests/valid_credentials.txt')
    azurebatch.input = {'': ['tests/valid_credentials.txt newdir']}
    azurebatch.job_name = 'pytest-2'
    resource_files = azurebatch.upload_input_to_blob_storage(input_id='')
    blob_client = BlockBlobService(account_name=azurebatch.storage_account_name,
                                   account_key=azurebatch.storage_account_key)
    generator = blob_client.list_blobs(container_name=azurebatch.job_name + '-input')
    blob_files = list()
    for blob in generator:
        blob_files.append(blob.name)
    assert len(blob_files) == 1
    assert resource_files[0].file_path == 'newdir/valid_credentials.txt'
    assert len(resource_files) == 1
    blob_client.delete_container(container_name=azurebatch.job_name + '-input')


def test_upload_dir():
    azurebatch = parse_configuration_file('tests/valid_credentials.txt')
    azurebatch.input = {'': ['tests/test_files']}
    azurebatch.job_name = 'pytest-3'
    resource_files = azurebatch.upload_input_to_blob_storage(input_id='')
    blob_client = BlockBlobService(account_name=azurebatch.storage_account_name,
                                   account_key=azurebatch.storage_account_key)
    generator = blob_client.list_blobs(container_name=azurebatch.job_name + '-input')
    blob_files = list()
    for blob in generator:
        blob_files.append(blob.name)
    assert len(blob_files) == 2
    resource_file_paths = list()
    for resource_file in resource_files:
        resource_file_paths.append(resource_file.file_path)
    assert 'test_files/file_1.txt' in resource_file_paths and 'test_files/file_2.txt' in resource_file_paths
    assert len(resource_files) == 2
    blob_client.delete_container(container_name=azurebatch.job_name + '-input')


def test_upload_dir_to_dest_dir():
    azurebatch = parse_configuration_file('tests/valid_credentials.txt')
    azurebatch.input = {'': ['tests/test_files newdir']}
    azurebatch.job_name = 'pytest-4'
    resource_files = azurebatch.upload_input_to_blob_storage(input_id='')
    blob_client = BlockBlobService(account_name=azurebatch.storage_account_name,
                                   account_key=azurebatch.storage_account_key)
    generator = blob_client.list_blobs(container_name=azurebatch.job_name + '-input')
    blob_files = list()
    for blob in generator:
        blob_files.append(blob.name)
    assert len(blob_files) == 2
    resource_file_paths = list()
    for resource_file in resource_files:
        resource_file_paths.append(resource_file.file_path)
    assert 'newdir/test_files/file_1.txt' in resource_file_paths and 'newdir/test_files/file_2.txt' in resource_file_paths
    assert len(resource_files) == 2
    blob_client.delete_container(container_name=azurebatch.job_name + '-input')


def test_upload_nested_dir_same_filenames():
    azurebatch = parse_configuration_file('tests/valid_credentials.txt')
    azurebatch.input = {'': ['tests/nested_dir']}
    azurebatch.job_name = 'pytest-5'
    resource_files = azurebatch.upload_input_to_blob_storage(input_id='')
    blob_client = BlockBlobService(account_name=azurebatch.storage_account_name,
                                   account_key=azurebatch.storage_account_key)
    generator = blob_client.list_blobs(container_name=azurebatch.job_name + '-input')
    blob_files = list()
    for blob in generator:
        blob_files.append(blob.name)
    assert len(blob_files) == 2
    resource_file_paths = list()
    for resource_file in resource_files:
        resource_file_paths.append(resource_file.file_path)
    assert 'nested_dir/file.txt' in resource_file_paths and 'nested_dir/subdir/file.txt' in resource_file_paths
    assert len(resource_files) == 2
    blob_client.delete_container(container_name=azurebatch.job_name + '-input')


