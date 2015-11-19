#!/usr/bin/env python

import argparse
import sys
import os
import csv
from uframe import UFrame

def main(args):
    
    if args.base_url:
        uframe_base = UFrame(base_url=args.base_url)
    else:
        uframe_env_url = os.getenv('UFRAME_BASE_URL')
        if uframe_env_url:
            uframe_base = UFrame(base_url=uframe_env_url)
        else:
            uframe_base = UFrame()
            
    fid = open(args.stream_csv, 'r')
    csv_reader = csv.reader(fid)
    
    required_cols = ['stream',
        'beginTime',
        'endTime',
        'sensor',
        'method']
    cols = csv_reader.next()
    
    # Make sure we have all of the required columns
    has_cols = [c for c in required_cols if c in cols]
    if len(has_cols) != len(required_cols):
        sys.stderr.write('Input csv file missing one or more columns: {:s}\n'.format(args.stream_csv))
        sys.stderr.flush()
        return 1
    
    c_range = range(0, len(cols))    
    for r in csv_reader:
        
        rd = {cols[i]:r[i] for i in c_range}
        tokens = rd['sensor'].split('-')
        if len(tokens) != 4:
            sys.stderr.write('Invalid sensor/reference designator specified: {:s}\n'.format(r['sensor']))
            sys.stderr.flush()
            continue
            
        async_url = '{:s}/{:s}/{:s}/{:s}-{:s}/{:s}/{:s}?beginDT={:s}&endDT={:s}&limit=-1&execDPA=true&format=application/netcdf&include_provenance=true'.format(
            uframe_base.url,
            tokens[0],
            tokens[1],
            tokens[2],
            tokens[3],
            rd['method'],
            rd['stream'],
            rd['beginTime'],
            rd['endTime'])
            
        sys.stdout.write('{:s}\n'.format(async_url))
        
    return 0
        
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('stream_csv',
            help='Filename containing stream request pieces.  Create this file using stream2ref_des_list.py')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.')
    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
