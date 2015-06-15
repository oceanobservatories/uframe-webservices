#! /usr/bin/env python

import argparse
from uframe import UFrame, get_arrays


def main(args):
    """
    Retrieve the list of available OOI arrays on the uFrame instance.

    Default uFrame instance is: http://uframe-test.ooi.rutgers.edu
    """

    if args.base_url:
        uframe_base = UFrame(base_url=args.base_url)
    else:
        uframe_base = UFrame()

    arrays = get_arrays(uframe_base=uframe_base)

    if not arrays:
        print 'No arrays found for uFrame instance: {:s}'.format(uframe_base.url)
        return

    print 'Available arrays:'
    for array in arrays:
        print array


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('-b', '--baseurl',
            dest='base_url',
            help='Specify an alternate uFrame server URL. Must start with \'http://\'.')

    parsed_args = arg_parser.parse_args()

    main(parsed_args)
