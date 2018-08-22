# Apparently this is how to import things if they don't end in .py
# This means that pytest has to be run from root of project dir.
exec(open('KubeJobSub/AzureBatch').read())
import pytest


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
