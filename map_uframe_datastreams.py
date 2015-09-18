#!/usr/bin/env python

from uframe import *
from uframe.availability import get_parameter_stream
import sys
import csv
import json
import argparse

def main(args):
    """
    Download metadata records for all available parameters and associated streams 
    (telemetered/recovered) from the default UFrame instance as CSV. The response
    is printed to STDOUT.

    The default uFrame instance is: http://uframe-test.ooi.rutgers.edu.  Other
    UFrame instances may be specified using the --baseurl option pointing to a 
    valid UFrame instance.
    """
    uframe = UFrame()
    if args.base_url:
        uframe = UFrame(base_url=args.base_url)
        
    if args.ref_des:
        stream_map = map_parameters_by_reference_designator(args.ref_des, uframe=uframe)
    else:
        stream_map = map_uframe_datastreams(args.array_id, uframe=uframe)
    
    if args.file_format == 'json':
        sys.stdout.write(json.dumps(stream_map))
        sys.stdout.flush()
    else:   
        stdout_writer = csv.writer(sys.stdout)
    
        column_map = {'parameter' : 'particleKey',
            'method' : 'stream method',
            'stream' : 'stream stream',
            'calculated' : '',
            'reference_designator' : 'stream sensor',
            'pdId' : 'pdId',
            'units' : 'units',
            'beginTime' : 'stream beginTime',
            'endTime' : 'stream endTime',
            'num_records' : 'stream count',
            'fillValue' : 'fillValue',
            'type' : 'type',
            'unsigned' : 'unsigned',
            'metadata_url' : 'metadata_url'}
            
        if args.all:
            
            cols = ['parameter',
                'method',
                'stream',
                'reference_designator',
                'pdId',
                'units',
                'beginTime',
                'endTime',
                'num_records',
                'fillValue',
                'type',
                'unsigned',
                'metadata_url']
        else:
            cols = ['parameter',
                'method',
                'stream',
                'calculated',
                'reference_designator']
                
            if args.particles:
                cols.append('num_records')
            if args.urls:
                cols.append('metadata_url')
            
            
        stdout_writer.writerow(cols)
        
        for stream in stream_map:
            
            row = []
            for col in cols:
                
                if col not in column_map.keys():
                    continue
                    
                tokens = column_map[col].split(' ')
                if len(tokens) == 1:
                    if col == 'calculated':
                        if stream['shape'] == 'FUNCTION':
                            row.append(1)
                        else:
                            row.append(0)
                    else:
                        row.append(stream[column_map[col]])
                else:
                    row.append(stream[tokens[0]][tokens[1]])
                
            stdout_writer.writerow(row)
        
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
                
                streams = map_streams(meta, url)
                
                for stream in streams:
                    stream_map.append(stream)
    
    return stream_map

def map_streams(meta, url, method=None):
    '''
    Return a stream map containing metadata for all streams and parameters 
    in the metadata (meta) array of dictionaries.
    
    Parameters:
        meta: a metdata response returned from a query of a UFrame instance for
            a specified instrument.  The format for the request is as follows:
                
                http://UFRAME_URL:12756/sensor/inv/SITE/NODE/INSTRUMENT/metadata
        url: the url used to retrieve the instrument stream/parameter metadata
            records from above.
        method: Return only streams for the specified method (i.e.: telemetered,
            'recovered', etc.)
            
    Returns:
        stream_map: array of dictionaries containing information on all parameters
            contained in the the meta argument.
    '''
    
    stream_map = []
    
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
    
def map_parameters_by_reference_designator(ref_des, uframe=UFrame()):
    '''
    Return a stream map containing metadata for all streams and parameters 
    for the specified reference designator.
    
    Parameters:
        ref_des: fully-qualified reference designator of the form:
            
            SITE-NODE-INSTRUMENT
        uframe: UFrame instance pointing the desired UFrame installation.
            
    Returns:
        stream_map: array of dictionaries containing information on all parameters
            contained in the the meta response.
    '''
    
    # Split the reference designtor on dashes
    r_tokens = ref_des.split('-')
    if len(r_tokens) != 4:
        sys.stderr.write('Invalid reference designator: {:s}\n'.format(ref_dest))
        sys.stderr.flush()
        return False
 
    metadata = get_sensor_metadata(r_tokens[0],
        r_tokens[1],
        '-'.join([r_tokens[2], r_tokens[3]]),
        uframe_base=uframe)  
        
    if not metadata:
        sys.stderr.write('No metadata found for: {:s}\n'.format(ref_des))
        sys.stderr.flush()
        return []
    
    # Create the metadata url
    url = '{:s}/{:s}/{:s}/{:s}/metadata'.format(
        uframe.url,
        r_tokens[0],
        r_tokens[1],
        '-'.join([r_tokens[2], r_tokens[3]]))    
    stream_map = map_streams(metadata, url)
    
    return stream_map
    
#def stream_map_to_csv(stream_map):
#    
#    stdout_writer = csv.writer(sys.stdout)
#    
#    cols = ['reference_designator',
#        'stream',
#        'method',
#        'parameter',
#        'pdId',
#        'units',
#        'beginTime',
#        'endTime',
#        'num_records',
#        'metadata_url']
#        
#    stdout_writer.writerow(cols)
#    
#    count = 0
#    for stream in stream_map:
#        
#        row = [stream['stream']['sensor'],
#            stream['stream']['stream'],
#            stream['stream']['method'],
#            stream['particleKey'],
#            stream['pdId'],
#            stream['units'],
#            stream['stream']['beginTime'],
#            stream['stream']['endTime'],
#            stream['stream']['count'],
#            stream['metadata_url']]
#            
#        stdout_writer.writerow(row)
#        count = count + 1
#        
#    return count
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('--array',
        help='Specify the name of the array to search for (i.e.: CE01ISSM).  If not specified, all arrays are downloaded.',
        dest='array_id',
        default=None)
    arg_parser.add_argument('-r', '--refdes',
        help='Map streams for a single reference designator (i.e.: CE01ISSM-MFD35-02-PRESFA000)',
        dest='ref_des',
        default = None)
    arg_parser.add_argument('-s', '--subsite',
        dest='subsite',
        help='Restrict results to the specified subsite (i.e.: GI01SUMO).')
    arg_parser.add_argument('-p', '--particles',
        dest='particles',
        action='store_true',
        help='Print created particle diagnostics.')
    arg_parser.add_argument('-a', '--all',
        help = 'Print entire metadata record.  Default is false and results in printing stream/parameter info only.',
        dest='all',
        action='store_true')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.')
    arg_parser.add_argument('-f', '--format',
        dest='file_format',
        default='csv',
        help='Specify the response type format (\'csv\' <Default> or \'json\').')
    arg_parser.add_argument('-u', '--url',
        help = 'Print the instrument metadata stream url.',
        dest = 'urls',
        action = 'store_true')
    
    parsed_args = arg_parser.parse_args()

    num_streams = main(parsed_args)
