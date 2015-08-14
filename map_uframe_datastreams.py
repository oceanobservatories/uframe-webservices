#!/usr/bin/env python

from uframe import *
import sys
import csv
import json
import argparse

sys.path.append('/Users/kerfoot/code/ooi')
from testing.availability import get_parameter_stream

def main(args):
    """
    Download metadata records for all available parameters and associated streams 
    (telemetered/recovered) from the default UFrame instance as CSV (default) or 
    JSON.  Responses are printed to STDOUT.

    The default uFrame instance is: http://uframe-test.ooi.rutgers.edu
    """
    uframe = UFrame()
    if args.base_url:
        uframe = UFrame(base_url=args.base_url)
        
    stream_map = map_uframe_datastreams(args.array_id, uframe=uframe)
    
    if args.file_format == 'json':
        json.dumps(stream_map)
    else:   
        stream_map_to_csv(stream_map)
        
    return len(stream_map)
    
def map_uframe_datastreams(array_id=None, method=None, uframe=UFrame()):
    """
    Download metadata records for all available parameters and associated streams 
    (telemetered/recovered) from the default UFrame instance as CSV (default) or 
    JSON.

    The default uFrame instance is: http://uframe-test.ooi.rutgers.edu
    """
    stream_map = []
    
    # Get the list of available array names
    arrays = get_arrays(uframe_base=uframe)
    if not arrays:
        sys.stderr.write('UFrame instance contains no arrays\n')
        sys.stderr.flush()
        return stream_map
    
    if array_id:
        if array_id not in arrays:
            sys.stderr.write('Invalid array specified: {:s}\n'.format(array_id))
            sys.stderr.flush()
            return stream_map
        else:
            arrays = [array_id]
            
    for array_id in arrays:
        
        #sys.stdout.write('Fetching Stream: {:s}\n'.format(array_id))
        
        # Get the list of available platforms for the current array_id
        platforms = get_platforms(array_id, uframe_base=uframe)
        
        if not platforms:
            sys.stderr.write('{:s}: Array contains no platforms\n'.format(array_id))
            sys.stderr.flush()
            continue
            
        for platform in platforms:
            
            #sys.stdout.write('Fetching Platform: {:s}\n'.format(platform))
            
            # Get the list of sensors for this platform
            sensors = get_platform_sensors(array_id, platform, uframe_base=uframe)
            
            if not sensors:
                sys.stderr.write('{:s}-{:s}: Platform contains no sensors'.format(array_id, platform))
                sys.stderr.flush()
                continue
                
            for sensor in sensors:
                
                #sys.stdout.write('Fetching Sensor: {:s}\n'.format(sensor))
                
                # Get the metadata for this sensor
                meta = get_sensor_metadata(array_id, platform, sensor, uframe_base=uframe)
                
                if not meta:
                    sys.stderr.write('{:s}-{:s}-{:s}: Sensor contains no particleKeys'.format(array_id, platform, sensor))
                    sys.stderr.flush()
                    continue
                    
                # Create the metadata url
                url = '{:s}/{:s}/{:s}/{:s}/metadata'.format(
                    uframe.url,
                    array_id,
                    platform,
                    sensor)
                
                seen_parameters = []    
                for parameter in meta['parameters']:
                    
                    if parameter['particleKey'] in seen_parameters:
                        continue
                        
                    seen_parameters.append(parameter['particleKey'])
                    
                    streams = []
                    for t in range(len(meta['times'])):
                        if method:
                            if meta['times'][t]['stream'] == parameter['stream'] and meta['times'][t]['method'] == method:
                                streams.append(meta['times'][t])
                        else:
                            if meta['times'][t]['stream'] == parameter['stream']:
                                streams.append(meta['times'][t])
                                
                    for stream in streams:
                        param_copy = parameter.copy()
                        param_copy['stream'] = stream 
                        param_copy['metadata_url'] = url
                        stream_map.append(param_copy)
    
    return stream_map
                    
def stream_map_to_csv(stream_map):
    
    stdout_writer = csv.writer(sys.stdout)
    
    cols = ['reference_designator',
        'stream',
        'method',
        'parameter',
        'pdId',
        'units',
        'beginTime',
        'endTime',
        'num_records',
        'metadata_url']
        
    stdout_writer.writerow(cols)
    
    count = 0
    for stream in stream_map:
        
        row = [stream['stream']['sensor'],
            stream['stream']['stream'],
            stream['stream']['method'],
            stream['particleKey'],
            stream['pdId'],
            stream['units'],
            stream['stream']['beginTime'],
            stream['stream']['endTime'],
            stream['stream']['count'],
            stream['metadata_url']]
            
        stdout_writer.writerow(row)
        count = count + 1
        
    return count
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('--array',
            help='Specify the name of the array to search for (i.e.: CE01ISSM).  If not specified, all arrays are downloaded.',
            dest='array_id',
            default=None)
    arg_parser.add_argument('-b', '--baseurl',
            dest='base_url',
            help='Specify an alternate uFrame server URL. Must start with \'http://\'.')
    arg_parser.add_argument('-f', '--format',
            dest='file_format',
            default='csv',
            help='Specify the response type format (\'csv\' <Default> or \'json\').')
    
    parsed_args = arg_parser.parse_args()

    num_streams = main(parsed_args)
