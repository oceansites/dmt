#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
import logging
from thredds_crawler.crawl import Crawl
from compliance_checker.runner import ComplianceChecker, CheckSuite

# Load all available checker classes
check_suite = CheckSuite()
check_suite.load_all_available_checkers()

def main(args):
    if args.format == 'summary':
        hdr_fmt = '{},' * len(args.test)
        rpt_fmt = '{:.1f},' * len(args.test)
        report_fmt = '{},' + rpt_fmt[:-1]
        print(('{},' + hdr_fmt[:-1]).format('url', *sorted(args.test)))

    for cat in args.catalog_url:
        select_str = ''
        if args.pattern:
            select_str = r'.*{}.*'.format(args.pattern)

        if args.verbose:
            crawl_log = logging.getLogger('thredds_crawler')
            crawl_log.setLevel(logging.DEBUG)
            handler = logging.FileHandler('thredds_crawler.log')
            formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
            handler.setFormatter(formatter)
            crawl_log.addHandler(handler)

        c = Crawl(cat, select=[select_str], debug=False)

        for url in (s.get("url") for d in c.datasets 
                                        for s in d.services 
                                            if s.get("service").lower() == "opendap"):

            if args.format == 'summary':
                cs = CheckSuite()
                if args.criteria == 'normal':
                    limit = 2
                elif args.criteria == 'strict':
                    limit = 1
                elif args.criteria == 'lenient':
                    limit = 3
                ds = cs.load_dataset(url)
                score_groups = cs.run(ds, *args.test)

                # Always use sorted test (groups) so they print in correct order
                reports = {}
                for checker, rpair in sorted(score_groups.items()):
                    groups, _ = rpair
                    _, points, out_of = cs.get_points(groups, limit)
                    reports[checker] = (100 * float(points) / float(out_of))
                
                print((report_fmt).format(url, *[reports[t] for t in sorted(args.test)]))
            else:
                # Send the compliance report to stdout
                ComplianceChecker.run_checker(url, args.test, args.verbose, args.criteria,
                                              args.output, args.format)

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
                        help="Increase output. May be specified up to three times; thredds_crawler.log created in current dir.",
                        action="count",
                        default=0)

    parser.add_argument('-f', '--format', default='text',
                        choices=['text', 'html', 'json', 'summary'], help='Output format')
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

