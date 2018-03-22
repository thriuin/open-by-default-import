
from ckan.logic import NotFound
from ckanapi import RemoteCKAN
import argparse
import hashlib
from datetime import datetime
from dateutil import parser as dateparser
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
parser.add_argument('resource_file_dir', help='Directory that contains the resource files')
args = parser.parse_args()

ua = 'obd-uploader/1.0 (+http://open.canada.ca/ckan)'
with RemoteCKAN('https://pilot.open.canada.ca/ckan', user_agent=ua) as ckan_instance:
    with open(args.json_lines_file, 'r') as jl_file:
        for jl_line in jl_file:
            obd_record = json.loads(jl_line)

            last_modified = dateparser.parse(obd_record['date_modified'])
            try:
                ckan_record = ckan_instance.action.package_show(id=obd_record['id'])
                assert len(ckan_record['resources']) == 1
                obd_resource = ckan_record['resources'][0]
                # Use the resource hash to decide if the file needs to be updated


            except NotFound:
                print('Cannot find record {0}: {1}'.format(obd_record['id'], obd_record['title_translated']['en']))
                continue
            exit(0)

