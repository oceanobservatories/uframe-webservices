#! /usr/bin/env python

import argparse
import sys
from uframe import UFrame, get_arrays, get_platforms, get_platform_sensors


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
    
    results = []    
    if args.refdes:
        for array in arrays:
            subsites = get_platforms(array, uframe_base=uframe_base)
            for subsite in subsites:
                instruments = get_platform_sensors(array, subsite, uframe_base=uframe_base)
                for instrument in instruments:
                    results.append('{:s}-{:s}-{:s}\n'.format(array, subsite, instrument))
    else:
        results = arrays
        
    for result in results:
        sys.stdout.write('{:s}\n'.format(result))
            


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.')
    arg_parser.add_argument('-r', '--refdes',
        dest='refdes',
        action='store_true',
        help='Create a list of all all fully qualified reference designators')
    parsed_args = arg_parser.parse_args()

    main(parsed_args)
