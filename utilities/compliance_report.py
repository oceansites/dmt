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

import argparse
from thredds_crawler.crawl import Crawl
from compliance_checker.runner import ComplianceChecker, CheckSuite

# Load all available checker classes
check_suite = CheckSuite()
check_suite.load_all_available_checkers()

def main(args):
    for cat in args.catalog_url:
        select_str = ''
        if args.pattern:
            select_str = r'.*{}.*'.format(args.pattern)

        if args.verbose:
            c = Crawl(cat, select=[select_str], debug=True)
        else:
            c = Crawl(cat, select=[select_str], debug=False)

        for url in (s.get("url") for d in c.datasets 
                                        for s in d.services 
                                            if s.get("service").lower() == "opendap"):

            # Send the compliance report to stdout
            return_value, errors = ComplianceChecker.run_checker(
                                        url, ['cf', 'acdd'], True, 'normal', '-', 'text')

def parse_command_line():

    parser = argparse.ArgumentParser()
    parser.add_argument('--pattern', '-p', '--pattern=', '-p=', 
                        help="Select text for thredds_crawler: 'select=[.*<pattern>.*]'")
    parser.add_argument('--test', '-t', '--test=', '-t=', default=('acdd',),
                        nargs='+',
                        choices=sorted(check_suite.checkers.keys()),
                        help="Select the Checks you want to perform.  Defaults to 'acdd' if unspecified")

    parser.add_argument('--criteria', '-c',
                        help="Define the criteria for the checks.  Either Strict, Normal, or Lenient.  Defaults to Normal.",
                        nargs='?', default='normal',
                        choices = ['lenient', 'normal', 'strict'])

    parser.add_argument('--verbose', '-v',
                        help="Increase output. May be specified up to three times.",
                        action="count",
                        default=0)

    parser.add_argument('-f', '--format', default='text',
                        choices=['text', 'html', 'json'], help='Output format')
    parser.add_argument('-o', '--output', default='-', action='store',
                        help='Output filename')
    parser.add_argument('-V', '--version', action='store_true',
                        help='Display the IOOS Compliance Checker version information.')
    parser.add_argument('catalog_url', nargs='*',
                        help="The THREDDS Catalog URL ending in '.xml'.")

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_command_line()
    main(args)

