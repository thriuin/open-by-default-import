# open-by-default-import

Code for importing content into the Open by Default project

Open by default (hhttps://open.canada.ca/en/open-by-default-pilot) is a pilot project by the Government of Canada to
provide public access to documents as they are being created.

The technology behind Open by Default is CKAN (http://github.com/open-data/ckanext-canada),
the same technology that powers the Open Government Portal (https://open.canada.ca/data/en/dataset).

Open by Default uses a slight different combination of extensions than our main portal:


ckan.plugins = canada_forms
               canada_obd
               canada_package
               wet_boew_gcweb
               scheming_datasets
               fluent
               cloudstorage
               extractor

When setting up CKAN for Open by Default, be sure to run the ckan-extractor setup script: ex.

paster --plugin=ckanext-extractor init -c /etc/ckan/default/production.ini

See:  https://github.com/stadt-karlsruhe/ckanext-extractor for details