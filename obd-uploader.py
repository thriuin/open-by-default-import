
from ckan.logic import NotFound
from ckanapi import RemoteCKAN
import argparse
import ConfigParser
import hashlib
from datetime import datetime
from azure.common import AzureMissingResourceHttpError
from azure.storage.blob import BlockBlobService
from dateutil import parser as dateparser
from tempfile import mkdtemp

import os
import simplejson as json


def md5(file_to_hash):
    hash_md5 = hashlib.md5()
    with open(file_to_hash, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def sha384(file_to_hash):
    hash_sha = hashlib.sha384()
    with open(file_to_hash, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha.update(chunk)
    return hash_sha.hexdigest()

parser = argparse.ArgumentParser(description='Add or update Open by Default resources')
parser.add_argument('json_lines_file', help='ObD JSON Lines file')
parser.add_argument('source_container', help='Directory that contains the resource files from Open by Default')
parser.add_argument('remote_ckan_url', help='CKAN Instance for Open by Default')
args = parser.parse_args()


Config = ConfigParser.ConfigParser()
Config.read('azure.ini')

block_blob_service =  BlockBlobService(Config.get('azure-blob-storage', 'account_name'),
                                       Config.get('azure-blob-storage', 'account_key'))

download_ckan_dir = mkdtemp()
download_gcdocs_dir = mkdtemp()

ua = 'Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0'
with RemoteCKAN(args.remote_ckan_url, user_agent=ua) as ckan_instance:
    with open(args.json_lines_file, 'r') as jl_file:
        for jl_line in jl_file:
            obd_record = json.loads(jl_line)
            obd_resource_name = ''
            last_modified = dateparser.parse(obd_record['date_modified'])
            try:
                ckan_record = ckan_instance.action.package_show(id=obd_record['id'])
                assert len(ckan_record['resources']) == 1
                obd_resource_name = 'resources/{0}/{1}'.format(ckan_record['resources'][0]['id'],
                                                     ckan_record['resources'][0]['name']).lower()
                obd_gcdocs_name = 'Documents/{0}'.format(ckan_record['resources'][0]['name'].lower())
                # Use the resource hash to decide if the file needs to be updated. Retrieve the file from Azure and
                # compare to latest file from GCDocs

                # 1. Get the current published file and hash it
                local_ckan_file = os.path.join(download_ckan_dir, os.path.basename(ckan_record['resources'][0]['name']))
                block_blob_service.get_blob_to_path('archive-queue', obd_resource_name, local_ckan_file)
                ckan_sha = sha384(local_ckan_file)

                # 2. Get the uploaded file and hash it
                local_gcdocs_file = os.path.join(download_gcdocs_dir, os.path.basename(ckan_record['resources'][0]['name']))
                block_blob_service.get_blob_to_path('opengovtestqueue', obd_gcdocs_name, local_gcdocs_file)
                gcdocs_sha = sha384(local_gcdocs_file)

                if ckan_sha == gcdocs_sha:
                    print "No file update required"

                else:
                    print "Files differ!"
                    # Upload files

                # Update metadata?


                os.remove(local_ckan_file)
                os.remove(local_gcdocs_file)
            except NotFound:
                print('Cannot find record {0}: {1}'.format(obd_record['id'], obd_record['title_translated']['en']))
                continue
            except AzureMissingResourceHttpError as amrh_ex:
                print('Cannot find blob: {0}'.format(obd_resource_name))
                continue
            except Exception as ex:
                print ex.message
            # @TODO Remove - Temp
            break

os.rmdir(download_ckan_dir)
os.rmdir(download_gcdocs_dir)
exit(0)

