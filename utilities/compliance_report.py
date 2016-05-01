#!/usr/bin/env python
'''
Script to provide OceanSITES compliance report for files in the GDAC

Mike McCann
30 April 2016
'''

import sys

# Temporary. Use local thredds-crawler and compliance-checker 
# See: https://github.com/asascience-open/thredds_crawler/issues/16
sys.path.insert(0, '/home/vagrant/dev/thredds_crawler')
sys.path.insert(0, '/home/vagrant/dev/compliance-checker')

from thredds_crawler.crawl import Crawl
from compliance_checker.runner import ComplianceChecker, CheckSuite

# Load all available checker classes
check_suite = CheckSuite()
check_suite.load_all_available_checkers()

site_url = 'http://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/MBARI/catalog.xml'

# Loop through OPeNDAP URLS for files from the GDAC
##c = Crawl(site_url, select=[".*.nc$"], debug=True)
c = Crawl(site_url, select=[".*OS_MBARI-M2_20100402_R_TS.nc$"], debug=True)
for url in (s.get("url") for d in c.datasets 
                                for s in d.services 
                                    if s.get("service").lower() == "opendap"):

    url = 'http://dods.ndbc.noaa.gov/thredds/dodsC/oceansites/DATA/MBARI/OS_MBARI-M0_20040604_R_TS.nc'
    # Send the compliance report to stdout
    return_value, errors = ComplianceChecker.run_checker(
                                url, ['cf', 'acdd'], None, 'normal', '-', 'text')
