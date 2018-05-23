
import ConfigParser
import hashlib
import logging
import os
import requests.exceptions
import simplejson as json
from azure.common import AzureMissingResourceHttpError
from azure.storage.blob import BlockBlobService
from azure.storage.blob.models import ResourceProperties
from ckan.logic import NotFound
from ckanapi import RemoteCKAN
from datetime import datetime
from dateutil import parser as dateparser
from tempfile import mkdtemp


# Read configuration information and initialize

Config = ConfigParser.ConfigParser()
Config.read('azure.ini')

ckanjson_dir = Config.get('working', 'ckanjson_directory')

block_blob_service = BlockBlobService(Config.get('azure-blob-storage', 'account_name'),
                                      Config.get('azure-blob-storage', 'account_key'))

ckan_container = Config.get('azure-blob-storage', 'account_obd_container')
gcdocs_container = Config.get('azure-blob-storage', 'account_gcdocs_container')
doc_intake_dir = Config.get('working', 'intake_directory')
archive_container = Config.get('azure-blob-storage', 'account_archive_container')

# Setup logging

logger = logging.getLogger('base')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] obd3 "%(message)s"')
ch.setFormatter(formatter)
logger.addHandler(ch)


def md5(file_to_hash):
    """
    Get an md5 hash value for a file.
    :param file_to_hash: path to file to hash
    :return: md5 hash value or None if the file could not be found
    """

    hash_md5 = hashlib.md5()
    if os.path.isfile(file_to_hash):
        with open(file_to_hash, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    else:
        logger.warn("md5(): File not found: {0}".format(file_to_hash))
        return None


def sha384(file_to_hash):
    """
    Get a SHA 384 hash value for a file.
    :param file_to_hash: path to file to hash
    :return: SHA 384 hash value or None if the file could not be found
    """

    hash_sha = hashlib.sha384()
    if os.path.isfile(file_to_hash):
        with open(file_to_hash, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha.update(chunk)
        return hash_sha.hexdigest()
    else:
        logger.warn("sha384() File not found: {0}".format(file_to_hash))
        return None


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


def add_ckan_record(package_dict):
    """
    Add a new dataset to the Open by Default Portal
    :param package_dict: JSON dict of the new package
    :return: The created package
    """

    remote_ckan_url = Config.get('ckan', 'remote_url')
    remote_ckan_api = Config.get('ckan', 'remote_api_key')
    user_agent = Config.get('web', 'user_agent')
    new_package = None

    with RemoteCKAN(remote_ckan_url, user_agent=user_agent, apikey=remote_ckan_api) as ckan_instance:
        try:
            new_package = ckan_instance.action.package_create(**package_dict)
        except Exception as ex:
            logger.error("add_ckan_record(): {0}".format(ex.message))
    return new_package


def update_ckan_record(package_dict):
    """
    Add a new dataset to the Open by Default Portal
    :param package_dict: JSON dict of the new package
    :return: The created package
    """

    remote_ckan_url = Config.get('ckan', 'remote_url')
    remote_ckan_api = Config.get('ckan', 'remote_api_key')
    user_agent = Config.get('web', 'user_agent')
    new_package = None

    with RemoteCKAN(remote_ckan_url, user_agent=user_agent, apikey=remote_ckan_api) as ckan_instance:
        try:
            new_package = ckan_instance.action.package_patch(**package_dict)
        except Exception as ex:
            logger.error("update_ckan_record(): {0}".format(ex.message))
    return new_package


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


def update_resource(package_id, resource_file):
    """
    Add or update the resource file for the dataset
    :param package_id: OBD dataset ID
    :param resource_file: path to the resource file
    :return: Nothing
    """

    remote_ckan_url = Config.get('ckan', 'remote_url')
    remote_ckan_api = Config.get('ckan', 'remote_api_key')
    user_agent = Config.get('web', 'user_agent')
    with RemoteCKAN(remote_ckan_url, user_agent=user_agent, apikey=remote_ckan_api) as ckan_instance:
        try:
            package_record = ckan_instance.action.package_show(id=package_id)
        except NotFound as nf:
            logger.error("update_resource(): {0}".format(nf.message))
            return

        if len(package_record['resources']) == 0:
            ckan_instance.action.resource_create(package_id=package_id,
                                                 url='',
                                                 upload=open(resource_file, 'rb'))
            logger.info("update_resource(): added resource to {0}".format(package_id))
        else:
            ckan_instance.action.resource_patch(id=package_record['resources'][0]['id'],
                                                url='',
                                                upload=open(resource_file, 'rb'))
            logger.info("update_resource(): updated resource {0}".format(package_record['resources'][0]['id']))


def get_blob(container, blob_name, local_name):
    """
    Copy of file from Azure blob storage to the local file system
    :param container: Azure Blob Storage container name
    :param blob_name: Azure Blob file name
    :param local_name: Local file name
    :return: Blob object or None if the blob could not be copied
    """
    blob = None
    try:
        blob = block_blob_service.get_blob_to_path(container, blob_name, local_name)
    except AzureMissingResourceHttpError as amrh_ex:
        logger.info('get_blob(): Cannot find blob: {0}'.format(obd_resource_name))
        logger.error("get_blob(): ".format(amrh_ex.message))
    except Exception as ex:
        logger.error("get_blob(): ".format(ex.message))
    return blob


def put_blob(container, blob_name, local_name):
    """
    Upload a file to Azure blob storage
    :param container: Azure container name
    :param blob_name: Full name of the blob to create
    :param local_name: Path of the file to upload
    :rtype ResourceProperties
    :return: Properties of uploaded blob
    """
    success = False
    try:
        block_blob_service.create_blob_from_path(container, blob_name, local_name, max_connections=4)
        # Verify
        success = block_blob_service.exists(container, blob_name=blob_name)
    except Exception as ex:
        logger.error("get_blob(): ".format(ex.message))
    return success


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


def get_gcdoc_name_root(blob_name):
    """
    Get the root of a file name exported from GCDOCS
    :param blob_name: a file name ex. 123.doc or 123.doc.xml
    :return: File name root or None if one is not found
    """
    key_split = blob_name.split('.')
    if len(key_split) > 0:
        return key_split[0]
    else:
        return None


def get_gcdoc_blob_list():
    """
    Get a list of files that were uploaded by GCDocs. Key them by the unique GCDocs Identifier
    :rtype: dict
    :return A dictionary of blob names that use the GCDocs ID as the key (ex. obd-gc0001-123456)
    """
    # Create a dictionary of the files in the container. Each entry is a list of blobs associated with the document ID
    blob_dict = {}
    blobs = block_blob_service.list_blobs(gcdocs_container)
    for blob in blobs:
        key = get_gcdoc_name_root(blob.name)
        if key and key in blob_dict:
            key_list = blob_dict[key]
            key_list.append(blob.name)
            blob_dict[key] = key_list
        else:
            key_list = [blob.name]
            blob_dict[key] = key_list
    return blob_dict


# Set up for interacting with Azure
gcdocs_blobs = get_gcdoc_blob_list()
processed_gcdocs = []
download_ckan_dir = mkdtemp()


def archive_blobs(blob_prefix, timestamp=None):
    """
    Copy the files that were processed in this pass to another container for backup
    :param blob_prefix: GCDocs Identifier
    :param timestamp: A timestamp used to set up the archive folder
    :rtype int
    :return: Number of files archived
    """
    no_files = 0
    if blob_prefix in gcdocs_blobs:
        files_to_move = gcdocs_blobs[blob_prefix]
        if not timestamp:
            timestamp = datetime.utcnow()
        archive_date = timestamp.strftime("%Y-%m-%d")
        archive_time = timestamp.strftime("%H-%M")
        for gcdocs_file in files_to_move:
            local_azure_file = os.path.join(download_ckan_dir, gcdocs_file)
            b = get_blob(gcdocs_container, gcdocs_file, local_azure_file)
            if b:
                archive_file = os.path.split(gcdocs_file)[1]
                archive_path = os.path.join(archive_date, archive_time, archive_file)
                if put_blob(archive_container, archive_path, local_azure_file):
                    delete_blob(gcdocs_container, gcdocs_file)
                os.remove(local_azure_file)
                no_files += 1
    return no_files


# initialize working variables

jsonl_file_list = []
this_moment = datetime.utcnow()

# Get a list of JSON line files to process
for root, dirs, files in os.walk(ckanjson_dir):
    for json_file in files:
        if json_file.endswith(".jsonl"):
            jsonl_file_list.append((os.path.join(root, json_file)))
assert len(jsonl_file_list) > 0, "Nothing to import - no files found."

# Process all Json lines input files
for ckan_input in jsonl_file_list:
    with open(ckan_input, 'r') as jl_file:
        for jl_line in jl_file:
            obd_record = json.loads(jl_line)

            obd_record_key = get_gcdoc_name_root(obd_record['resources'][0]['name_translated']['en'])  # type: str

            # Verification check - do not post documents that have already expired. Remove it from the portal
            # if it was uploaded before.
            expiry_date = dateparser.parse(obd_record['date_expires'])
            if expiry_date <= datetime.utcnow():
                logger.warn('This record has already expired')

                # If the dataset exists, then delete the resource and the dataset.
                delete_ckan_record(obd_record['id'])
                archive_blobs(obd_record_key, timestamp=this_moment)
                continue

            # Get the current published file from the OBD Portal. It may not exist if this is the first
            # time the document has been posted to the portal

            ckan_record = get_ckan_record(obd_record['id'])

            # If the record does not exist, then add the document to the OBD Portal. This new record will have
            # a placeholder resource record.
            if len(ckan_record) == 0:
                ckan_record = add_ckan_record(obd_record)

            # If this record has more than one resource, it cannot be an Open by Default record

            num_of_resources = 0
            if 'resources' in ckan_record:
                num_of_resources = len(ckan_record['resources'])

            if num_of_resources > 1:
                print('More than one resource found for dataset: {0}'.format(ckan_record['id']))
                archive_blobs(obd_record_key, timestamp=this_moment)
                break

            local_gcdocs_file = os.path.join(doc_intake_dir,
                                             os.path.basename(ckan_record['resources'][0]['name']))
            # Set the file size in the CKAN record
            ckan_record['resources'][0]['size'] = str(os.path.getsize(local_gcdocs_file) / 1024)

            # Check if the resource already exists or not. If it does, download a copy and compare with the
            # currently uploaded file. If they are the same, no further action is required. If not, then update.
            if num_of_resources == 1:
                obd_resource_name = 'resources/{0}/{1}'.format(ckan_record['resources'][0]['id'],
                                                               ckan_record['resources'][0]['name'])

                local_ckan_file = os.path.join(download_ckan_dir,
                                               os.path.basename(ckan_record['resources'][0]['name']))
                # Ensure we can retrieve the resource
                if not get_blob(ckan_container, obd_resource_name, local_ckan_file):
                    # The Azure API may create blank files
                    if os.path.exists(local_ckan_file):
                        os.remove(local_ckan_file)
                    local_ckan_file = None

                if local_ckan_file:
                    ckan_sha = sha384(local_ckan_file)
                    if not ckan_sha:
                        logger.error('Unable to generate SHA 348 Hash for file {0}'.format(local_ckan_file))
                        # If this is happening, best to quit and investigate
                        break
                else:
                    ckan_sha = ''

                # Get the uploaded file and hash it

                gcdocs_sha = sha384(local_gcdocs_file)
                if not gcdocs_sha:
                    logger.error('Unable to generate SHA 348 Hash for file {0}'.format(local_gcdocs_file))
                    # If this is happening, best to quit and investigate
                    break

                if ckan_sha == gcdocs_sha:
                    logger.info("No file update required for {0}".format(obd_record['id']))

                else:
                    logger.info("Updated file from GCDocs for {0}".format(obd_record['id']))
                    # Upload file
                    update_resource(obd_record['id'], local_gcdocs_file)

                if local_ckan_file:
                    os.remove(local_ckan_file)

            else:
                update_resource(obd_record['id'], local_gcdocs_file)

            del obd_record['resources']
            update_ckan_record(obd_record)
            archive_blobs(obd_record_key, timestamp=this_moment)

            if os.path.exists(local_gcdocs_file):
                os.remove(local_gcdocs_file)

    # Save a copy of the JSON line file for audit purposes
    todays_date = this_moment.strftime("%Y-%m-%d")
    todays_time = this_moment.strftime("%H-%M")
    ckan_line_base = os.path.basename(ckan_input)
    ckan_line_archive = os.path.join(todays_date, todays_time, ckan_line_base)
    put_blob(archive_container, ckan_line_archive, ckan_input)
    os.remove(ckan_input)

os.rmdir(download_ckan_dir)
exit(0)
