from ckan.logic import NotFound
from ckan.lib.munge import munge_filename
from ckanapi.errors import CKANAPIError
from ckanapi import RemoteCKAN
import ConfigParser
import os
import requests.exceptions
import simplejson as json
import sys
from termcolor import cprint
import uuid


# Load Azure and file directory configuration information
Config = ConfigParser.ConfigParser()
Config.read('azure.ini')


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
            cprint('Record {0} does not exist'.format(record_id), 'yellow')
        except requests.exceptions.ConnectionError as ce:
            cprint('get_ckan_record(): Fatal connection error {0}'.format(ce.message), 'red', attrs=['blink'])
            exit(code=500)
        except CKANAPIError as ne:
            cprint('get_ckan_record(): Unexpected error {0}'.format(ne.message), 'yellow')

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
            cprint('Created new record {0}'.format(new_package['id']), 'green')
        except Exception as ex:
            cprint("Unable to create new portal record {0}".format(ex.message), 'yellow')
    return new_package


def update_resource(package_id, resource_file, idx):
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
            cprint("Unable to find record {0} to update".format(nf.message), 'yellow')
            return

        try:
            if len(package_record['resources']) < idx:
                ckan_instance.action.resource_create(package_id=package_id,
                                                     url='',
                                                     upload=open(resource_file, 'rb'))
                cprint("Added new resource to {0}".format(package_id), 'green')
            else:
                ckan_instance.action.resource_patch(id=package_record['resources'][idx]['id'],
                                                    url='',
                                                    upload=open(resource_file, 'rb'))
                cprint("Updated resource {0} for record {1}".format(idx, package_id), 'green')
        except CKANAPIError as ce:
            cprint(ce.message, 'yellow')


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
            cprint("Updated record {0}".format(package_dict['id']), 'green')
        except Exception as ex:
            cprint("Unable to update existing portal record: {0}".format(ex.message), 'red')
    return new_package


with open(sys.argv[1], 'r') as json_file:
    pkg = json.load(json_file)
    pkg_id = str(uuid.uuid5(uuid.NAMESPACE_URL, 'https://obd.open.canada.ca/' + os.path.splitext(sys.argv[1])[0]))
    pkg['id'] = pkg_id

    ckan_record = get_ckan_record(pkg_id)

    # If the record does not exist, then add the document to the OBD Portal. This new record will have
    # a placeholder resource record.
    if ckan_record is None or len(ckan_record) == 0:
        cprint('Adding new record {0}'.format(pkg['id']), 'green')
        add_ckan_record(pkg)
    else:
        cprint('Updating record {0}'.format(pkg['id']), 'green')
        update_ckan_record(pkg)

    i = 0
    for f in sys.argv[2:]:
        update_resource(pkg_id, f, i)
        i += 1
    cprint('Upload completed', 'green', attrs=['reverse'])
