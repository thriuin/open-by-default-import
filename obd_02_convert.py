import ConfigParser
import os
import simplejson as json
import uuid
import yaml
from datetime import datetime

oc_organizations = {
    "Canadian Heritage": '9EEB1859-D658-4E1B-A0E0-45CFAB4E3E5A',
    "Environment Canada": '49E2ADF4-AD7A-43EB-85C8-6433D37ED62C',
    "Natural Resources Canada": '9391E0A2-9717-4755-B548-4499C21F917B',
    "Treasury Board of Canada Secretariat": '81765FCD-32B3-4708-A593-3AA00705E62B'
}


def load_oc_resource_format():
    with open(os.path.join('schemas', 'presets.yaml'), 'r') as preset_file:
        presets = yaml.load(preset_file)
        resource_formats = {}
        resource_types = {}
        audience_types = {}
        for rec in presets['presets']:
            if rec['preset_name'] == 'canada_resource_format':
                for choice in rec['values']['choices']:
                    assert isinstance(choice, dict)
                    if 'mimetype' in choice:
                        resource_formats[choice['value']] = choice['mimetype']
                    else:
                        resource_formats[choice['value']] = ''
            elif rec['preset_name'] == 'canada_resource_type':
                for choice in rec['values']['choices']:
                    resource_types[choice['label']['en']] = choice['value']
            elif rec['preset_name'] == 'canada_audience':
                for choice in rec['values']['choices']:
                    audience_types[choice['label']['en']] = choice['value']
    return [resource_formats, resource_types, audience_types]


oc_resource_formats, oc_resource_types, oc_audience_types = load_oc_resource_format()


def convert(fields, filename):
    # Blank Dataset
    obd_ds = {'collection': 'publication',
              'id': str(uuid.uuid5(uuid.NAMESPACE_URL, 'http://obd.open.canada.ca/' + os.path.splitext(filename)[0]))}

    release_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not fields.get('date_published'):
        obd_ds['date_published'] = release_date

    obd_ds['state'] = 'active'
    obd_ds['type'] = 'doc'
    obd_ds['license_id'] = "ca-ogl-lgo"

    org_name = fields['Publisher Organization'].split('|')[0].strip()
    if org_name in oc_organizations:
        obd_ds['owner_org'] = oc_organizations[org_name]
    else:
        obd_ds['owner_org'] = oc_organizations['Treasury Board of Canada Secretariat']
    obd_ds['keywords'] = {}
    if 'Subject' in fields:
        keywords_by_lang = fields['Subject'].split('|')
        if len(keywords_by_lang) == 2:
            obd_ds['keywords']['en'] = keywords_by_lang[0].split(',')
            obd_ds['keywords']['fr'] = keywords_by_lang[1].split(',')
    if not fields.get('subject', None):
        obd_ds['subject'] = ["information_and_communications"]

    if 'Audience' in fields:
        if fields['Audience'] in oc_audience_types:
            obd_ds['audience'] = oc_audience_types[fields['Audience']]

    # Use unilingual titles where appropriate
    obd_ds['title_translated'] = {}
    if 'Title French' not in fields:
        obd_ds['title_translated']['fr'] = fields['Title English']
    else:
        obd_ds['title_translated']['fr'] = fields['Title French']
    if 'Title English' not in fields:
        obd_ds['title_translated']['en'] = fields['Title French']
    else:
        obd_ds['title_translated']['en'] = fields['Title English']

    obd_ds['doc_classification_code'] = fields['Classification Code']
    obd_ds['date_published'] = fields['Date Created']
    obd_ds['date_modified'] = fields['Date Modified']
    if 'Creator' in fields:
        obd_ds['creator'] = fields['Creator']
    obd_ds['notes_translated'] = {}
    if 'Description English' in fields:
        obd_ds['notes_translated']['en'] = fields['Description English']
    if 'Description French' in fields:
        obd_ds['notes_translated']['fr'] = fields['Description French']

    if 'Publisher Organization- Section' in fields:
        org_section = fields['Publisher Organization- Section'].split('|')
        if len(org_section) == 2:
            obd_ds['org_section']['en'] = org_section[0]
            obd_ds['org_section']['fr'] = org_section[1]

    # The Usage Condition is not currently being set.
    obd_ds['usage_condition'] = {}

    # The maintainer e-mail is not currently provided by OBD so it is set to
    # open-ouvert@tbs-sct.gc.ca
    obd_ds['maintainer_email'] = 'open-ouvert@tbs-sct.gc.ca'

    obd_res = {}
    res_name = os.path.basename(filename)
    obd_res['name_translated'] = {'en': res_name, 'fr': res_name}

    # Terrible Hack
    obd_res['format'] = filename.split('.')[1].upper()
    if obd_res['format'] not in oc_resource_formats:
        obd_res['format'] = 'other'

    # Placeholder - the file itself needs to be uploaded with the CKAN API
    obd_res['url'] = 'http://obd.open.canada.ca/' + filename

    obd_res['language'] = []
    if fields['Language'][:3] == 'fra':
        obd_res['language'].append('fr')
    if fields['Language'][:3] == 'eng':
        obd_res['language'].append('en')

    if not fields.get('Resource Type'):
        obd_res['resource_type'] = 'guide'
    else:
        obd_res_type = fields['Resource Type'].split('|')[0].strip()
        if obd_res_type in oc_resource_types:
            obd_res['resource_type'] = oc_resource_types[obd_res_type]
        else:
            obd_res['resource_type'] = 'guide'
    obd_ds['resources'] = [obd_res]
    return obd_ds


def main(file_list, dest_file):
    """
    Convert one or more metadata files from GCDocs to the CKAN format
    
    :type file_list: list
    :type dest_file: str
    """
    for json_filename in file_list:
        with open(json_filename, 'r') as json_filed:
            print json_filename
            fields = json.load(json_filed)
            obd_ds = convert(fields, fields['GCfile'])
            json_text = json.dumps(obd_ds)
        with open(dest_file, 'a') as output_file:
            output_file.write(json_text + '\n')


Config = ConfigParser.ConfigParser()
Config.read('azure.ini')

json_file_list = []
file_source = Config.get('working', 'intake_directory')
dest_dir = Config.get('working', 'ckanjson_directory')
file_output = datetime.now().strftime("ckan_obd_%Y-%m-%d_%H-%M-%S.jsonl")

# Read an individual file or a directory of .json files
# For this project, it will almost always be a directory
if os.path.isfile(file_source):
    json_file_list.append(file_source)
elif os.path.isdir(file_source):
    for root, dirs, files in os.walk(file_source):
        for json_file in files:
            if json_file.endswith(".json"):
                json_file_list.append((os.path.join(root, json_file)))

# Perform the conversion on one or more files
main(json_file_list, os.path.join(dest_dir, file_output))