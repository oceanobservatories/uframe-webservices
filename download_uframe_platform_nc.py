#! /usr/bin/env python

import argparse
from uframe import UFrame, get_uframe_array


def main(args):
    """
    Download NetCDF / JSON files for the most recent 1-day worth of data for telemetered
    and recovered data streams for the specified array_id.

    Default uFrame instance is: http://uframe-test.ooi.rutgers.edu
    """
    array_id = args.array_id

    if args.base_url:
        uframe_base = UFrame(base_url=args.base_url, timeout=args.timeout)
    else:
        uframe_base = UFrame(timeout=args.timeout)

    delattr(args, 'array_id')
    delattr(args, 'base_url')
    delattr(args, 'timeout')
    args.uframe_base = uframe_base

    fetched_urls = get_uframe_array(array_id, **vars(args))

    return fetched_urls


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('array_id',
            help='Name of the array to fetch')
    arg_parser.add_argument('-b', '--baseurl',
            dest='base_url',
            help='Specify an alternate uFrame server URL. Must start with \'http://\'.')
    arg_parser.add_argument('--timeout',
            type=int,
            default=10,
            help='Specify the timeout, in seconds (Default is 10 seconds).')
    arg_parser.add_argument('--format',
            dest='file_format',
            default='netcdf',
            help='Specify the format in which to download the files (\'netcdf\' <Default> or \'json\').')
    arg_parser.add_argument('--dpa_off',
            action='store_false',
            dest='exec_dpa',
            help='Do not execute data product algorithms (Default is On)')
    arg_parser.add_argument('-u', '--urlonly',
            action='store_true',
            help='Display the urls for the stream, but do not execute the download request')
    arg_parser.add_argument('-d', '--dest',
            dest='out_dir',
            help='Output files destination')
    arg_parser.add_argument('--deltatype',
            default='days',
            help='Type for calculating the subset start time, i.e.: years, months, weeks, days.  Must be a type kwarg accepted by dateutil.relativedelta.')
    arg_parser.add_argument('--deltavalue',
            dest='deltaval',
            type=int,
            default=1,
            help='Positive integer value to subtract from the end time to get the start time for subsetting.')
    arg_parser.add_argument('--provenance',
            action='store_true',
            dest='provenance',
            help='Request provenance values (Default is off)')
    arg_parser.add_argument('--nolimit',
            action='store_false',
            dest='limit',
            help='Turn data limit of 10,000 (default) off')

    parsed_args = arg_parser.parse_args()

    urls = main(parsed_args)

    if parsed_args.urlonly:
        for url in urls:
            print '{:s}'.format(url['url'])
    else:
        for url in urls:
            print '{:s},{:d},{:s},{:s}'.format(url['request_time'], url['code'], url['reason'], url['url'])
