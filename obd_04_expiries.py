
import ConfigParser
import logging
import requests.exceptions
from azure.common import AzureMissingResourceHttpError
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import ResourceProperties
from ckan.logic import NotFound
from ckanapi import RemoteCKAN
from datetime import datetime
from dateutil import parser as dateparser


# Read configuration information and initialize

Config = ConfigParser.ConfigParser()
Config.read('azure.ini')

block_blob_service = BlockBlobService(Config.get('azure-blob-storage', 'account_name'),
                                      Config.get('azure-blob-storage', 'account_key'))

ckan_container = Config.get('azure-blob-storage', 'account_obd_container')

# Setup logging

logger = logging.getLogger('base')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] obd4 "%(message)s"')
ch.setFormatter(formatter)
logger.addHandler(ch)


def get_ckan_record(record_id):
    """
    Retrieve a CKAN dataset record from a remote CKAN portal
    :param record_id: Unique Identifier for the dataset - For Open Canada, these are always UUID's
    :return: The CKAN package, or an empty dict if the dataset could not be retrieved
    """

    remote_ckan_url = Config.get('ckan', 'remote_url')
    user_agent = Config.get('web', 'user_agent')
    with RemoteCKAN(remote_ckan_url, user_agent=user_agent) as ckan_instance:
        package_record = {}
        try:
            package_record = ckan_instance.action.package_show(id=record_id)

        except NotFound:
            # This is a new record!
            logger.info('get_ckan_record(): Cannot find record {0}'.format(record_id))
        except requests.exceptions.ConnectionError as ce:
            logger.error('get_ckan_record(): Fatal connection error {0}'.format(ce.message))
            exit(code=500)

        return package_record


def delete_blob(container, blob_name):
    """
    Remove a file from blob storage
    :param container: Azure container name
    :param blob_name: Full name of the blob to create
    :return: Nothing
    """
    try:
        block_blob_service.delete_blob(container, blob_name)
    except Exception as ex:
        logger.error("get_blob(): ".format(ex.message))


def delete_ckan_record(package_id):
    """
    Remove a dataset and its associated resource from CKAN
    :param package_id:
    :return: Nothing
    """

    # First, verify and get the resource ID
    package_record = get_ckan_record(package_id)
    if len(package_record) == 0:
        logger.warn("delete_ckan_record(): cannot find record ID {0}".format(package_id))
        return

    # Get rid of the resource
    remote_ckan_url = Config.get('ckan', 'remote_url')
    remote_ckan_api = Config.get('ckan', 'remote_api_key')
    user_agent = Config.get('web', 'user_agent')

    with RemoteCKAN(remote_ckan_url, user_agent=user_agent, apikey=remote_ckan_api) as ckan_instance:
        try:
            delete_blob(ckan_container, 'resources/{0}/{1}'.format(package_record['resources'][0]['id'],
                                                                   package_record['resources'][0]['name'].lower()))
            ckan_instance.action.package_delete(id=package_record['id'])
            ckan_instance.action.dataset_purge(id=package_record['id'])
            logger.info("Deleted expired CKAN record {0}".format(package_record['id']))
        except Exception as ex:
            logger.error("delete_ckan_record(): {0}".format(ex.message))


obd_ckan_url = Config.get('ckan', 'remote_url')
obd_ua = Config.get('web', 'user_agent')
packages = []
offset = 0
right_now = datetime.utcnow()

with RemoteCKAN(obd_ckan_url, user_agent=obd_ua) as obd_ckan:
    while True:
        # Page through a list of all datasets on the site
        try:
            packages = obd_ckan.action.package_list(limit=100, offset=offset)
            if len(packages) == 0:
                break
            for dataset_id in packages:
                offset += 1
                package = get_ckan_record(dataset_id)
                if 'date_expires' in package:
                    try:
                        expires_on = dateparser.parse(package['date_expires'])
                        if expires_on <= right_now:
                            delete_ckan_record(package['id'])
                            logger.info("Deleted record {0} which expired on {1}".format(package['id'],
                                                                                         package['date_expires']))
                    except ValueError as ve:
                        logger.error(ve.message)

        except Exception as xx:
            logger.error(xx.message)
