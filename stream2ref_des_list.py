#! /usr/bin/env python

import argparse
import sys
import os
import csv
from uframe import UFrame, get_arrays, get_platforms, get_platform_sensors, get_sensor_metadata


def main(args):
    """
    Retrieve the list of available OOI arrays on the uFrame instance.

    The default uFrame instance is: http://uframe-test.ooi.rutgers.edu.  
    
    An alternate uFrame instance may be specified by setting the UFRAME_BASE_URL
    environment variable.
    """

    if args.base_url:
        uframe_base = UFrame(base_url=args.base_url)
    else:
        uframe_env_url = os.getenv('UFRAME_BASE_URL')
        if uframe_env_url:
            uframe_base = UFrame(base_url=uframe_env_url)
        else:
            uframe_base = UFrame()

    #sys.stdout.write('{:s}\n'.format(uframe_base))
    
    arrays = get_arrays(uframe_base=uframe_base)

    reference_designators = []
    
    if not arrays:
        sys.stderr.write('No arrays found for uFrame instance: {:s}\n'.format(uframe_base.url))
        sys.stderr.flush()
        return
    
    for array in arrays:

        platforms = get_platforms(array, uframe_base=uframe_base)
        
        if not platforms:
            sys.stderr.write('{:s}: No platforms found for array\n'.format(ref_des))
            sys.stderr.flush()
            continue
            
        for platform in platforms:
            
            ref_des = '{:s}-{:s}'.format(array, platform)
            
            sensors = get_platform_sensors(array, platform, uframe_base=uframe_base)
            
            if not sensors:
                sys.stderr.write('{:s}: No instruments found\n'.format(ref_des))
                sys.stderr.flush()
                continue
                
            for sensor in sensors:
                
                ref_des = '{:s}-{:s}-{:s}'.format(array, platform, sensor)
                
                meta = get_sensor_metadata(array, platform, sensor, uframe_base=uframe_base)
                if not meta:
                    sys.stderr.write('{:s}: No metadata found\n'.format(ref_des))
                    sys.stderr.flush()
                    continue
                    
                for t in meta['times']:
                    if t['stream'] != args.target_stream:
                        continue
                    
                    reference_designators.append(t)
                    
    if not reference_designators:
        sys.stderr.write('No reference designators found for stream: {:s}\n'.format(args.target_stream))
        sys.stderr.flush()
        return 1
        
    csv_writer = csv.writer(sys.stdout)
    cols = reference_designators[0].keys()
    csv_writer.writerow(cols)
    for r in reference_designators:
        v = [r[k] for k in cols]
        csv_writer.writerow(v)
        
    return 0

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('target_stream',
        help='Target stream name')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.')
    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
