
import ConfigParser
import logging
import os
import simplejson as json
import traceback
# noinspection PyPackageRequirements
from azure.storage.blob import BlockBlobService
from ckan.lib.munge import munge_filename
from datetime import datetime
from lxml import etree
from shutil import copyfile

# Load Azure and file directory configuration information

Config = ConfigParser.ConfigParser()
Config.read('azure.ini')

# Setup logging

logger = logging.getLogger('base')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
fh = logging.FileHandler(datetime.now().strftime(Config.get('working', 'error_logfile')))
ch.setLevel(logging.DEBUG)
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] obd_01 "%(message)s"')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)

def read_xml(filename):
    '''
    Read a GCDocs XML metadata export file, and convert the information to a generic object.
    :param filename: The path to the XML file to read
    :return: A python dictionary keyed on the dictionary name
    '''
    try:
        root = etree.parse(filename).getroot()
    except etree.XMLSyntaxError as xe:
        logger.error(xe.message)
        logger.error(traceback.format_exc())
        return None

    refs, extras = {}, {}
    assert (root.tag == "enterpriseLibrary")
    variant = root.find('application').find('parent').find('item').find(
        'variants').find('variant')
    start = variant.find('properties')

    for l in start.findall('property'):
        if l.find('value').text:
            refs[l.attrib['name']] = l.find('value').text
    for c in start.findall('propertyGroup'):
        assert (c.attrib['name'] == 'customMetadata')
        for row in c.findall('propertyRow'):
            r = {}
            for l in row.findall('property'):
                r[l.attrib['name']] = l.find('value').text
            if r['metadata'] and r['attribute']:
                refs[r['attribute']] = r['metadata']

    return refs


azure_account_name = Config.get('azure-blob-storage', 'account_name')
azure_account_key = Config.get('azure-blob-storage', 'account_key')
azure_gcdocs_container = Config.get('azure-blob-storage', 'account_gcdocs_container')
archive_directory = Config.get('working', 'archive_directory')

# Azure interface
block_blob_service = BlockBlobService(azure_account_name, azure_account_key)

# Create a local archive directory to hold a copy of the  XML metadata files and documents

timestamp = datetime.utcnow()
archive_folder = os.path.join(archive_directory, timestamp.strftime("%Y-%m-%d_%H-%M"))

# Download XML files from Azure and convert to JSON format
generator = block_blob_service.list_blobs(azure_gcdocs_container)
basedir = Config.get('working', 'intake_directory')
for blob in generator:
    # Don't create an archive directory unless needed
    if not os.path.exists(archive_folder):
        os.mkdir(archive_folder, 0o775)
    try:
        # Convert XML files to a simpler JSON files
        if os.path.splitext(blob.name)[1] == '.xml':
            source_name = os.path.splitext(os.path.basename(blob.name))[0]
            basename = os.path.splitext(source_name)[0]

            with open(os.path.join(basedir, "{0}.json".format(basename)), 'w') as jsonfile:
                logger.info('Downloading {0}'.format(os.path.basename(blob.name)))
                local_file = os.path.join(archive_folder, munge_filename(os.path.basename(blob.name)))
                assert isinstance(azure_gcdocs_container, str)
                b = block_blob_service.get_blob_to_path(azure_gcdocs_container, blob.name, local_file)
                if b:
                    block_blob_service.delete_blob(azure_gcdocs_container, blob.name)
                x_fields = read_xml(local_file)
                if x_fields:
                    x_fields['GCID'] = basename
                    x_fields['GCfile'] = source_name
                    jsonfile.write(json.dumps(x_fields, indent=4))

        # These deprecated indicator files no longer serve a purpose and can be deleted
        elif os.path.splitext(blob.name)[1] == '.ind':
            block_blob_service.delete_blob(azure_gcdocs_container, blob.name)

        # simply download and backup the document files
        else:
            local_file = os.path.join(basedir, munge_filename(os.path.basename(blob.name)))
            archive_file = os.path.join(archive_folder, os.path.basename(blob.name))
            b = block_blob_service.get_blob_to_path(azure_gcdocs_container, blob.name, local_file)
            if b:
                block_blob_service.delete_blob(azure_gcdocs_container, blob.name)
            copyfile(local_file, archive_file)

    except Exception as x:
        logger.error(traceback.format_exc())

