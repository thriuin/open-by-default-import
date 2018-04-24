
import ConfigParser
import os
import simplejson as json
# noinspection PyPackageRequirements
from azure.storage.blob import BlockBlobService
from lxml import etree
from shutil import rmtree
from tempfile import mkdtemp


def read_xml(filename):
    root = etree.parse(filename).getroot()

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
            if r['metadata']:
                extras.update(r)
    refs['extras'] = extras

    return refs


Config = ConfigParser.ConfigParser()
Config.read('azure.ini')

azure_account_name = Config.get('azure-blob-storage', 'account_name')
azure_account_key = Config.get('azure-blob-storage', 'account_key')
azure_gcdocs_container = Config.get('azure-blob-storage', 'account_gcdocs_container')

block_blob_service = BlockBlobService(azure_account_name, azure_account_key)

# Create a temp directory to hold imported XML metadata files
dir_path = mkdtemp()
print dir_path

# Download XML files from Azure and convert to JSON format
generator = block_blob_service.list_blobs(azure_gcdocs_container)
basedir = Config.get('working', 'intake_directory')
for blob in generator:
    if os.path.splitext(blob.name)[1] == '.xml':
        source_name = os.path.splitext(os.path.basename(blob.name))[0]
        basename = os.path.splitext(source_name)[0]

        with open(os.path.join(basedir, "{0}.json".format(basename).lower()), 'w') as jsonfile:
            print os.path.basename(blob.name)
            local_file = os.path.join(dir_path, os.path.basename(blob.name).lower())
            block_blob_service.get_blob_to_path(azure_gcdocs_container, blob.name, local_file)
            xfields = read_xml(local_file)
            xfields['GCID'] = basename
            xfields['GCfile'] = source_name
            # for xfield in xfields:
            #     print u"Ref {0}: {1}".format(xfield, xfields[xfield])
            # print ""
            jsonfile.write(json.dumps(xfields, indent=4))
    else:
  #      local_file = os.path.join(basedir, os.path.basename(blob.name).lower())
   #     block_blob_service.get_blob_to_path(azure_gcdocs_container, blob.name, local_file)
        pass

rmtree(dir_path)
