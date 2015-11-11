#! /usr/bin/env python

import argparse
import sys
import csv
import json
from uframe import UFrame, get_ref_des_streams


def main(args):
    """
    Retrieve all streams for the specified reference designator from the specified
    uFrame instance.

    Default uFrame instance is: http://uframe-test.ooi.rutgers.edu
    """

    if args.base_url:
        uframe_base = UFrame(base_url=args.base_url)
    else:
        uframe_base = UFrame()

    streams = get_ref_des_streams(args.ref_des, uframe_base=uframe_base)

    if not streams:
        sys.stderr.write('No arrays found for uFrame instance: {:s}'.format(uframe_base.url))
        return
    
    if args.file_format == 'json':
        sys.stdout.write(json.dumps(streams))
        return
    
    csv_writer = csv.writer(sys.stdout)
    for stream in streams:
        csv_writer.writerow(stream)
            


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('ref_des',
            help='reference designator')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.')
    arg_parser.add_argument('--format',
            dest='file_format',
            default='csv',
            help='Specify the format in which to download the files (\'csv\' <Default> or \'json\').')
    parsed_args = arg_parser.parse_args()

    main(parsed_args)
