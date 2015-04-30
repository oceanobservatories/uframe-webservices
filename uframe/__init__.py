'''
Module for querying uFrame instances and downloading responses, primarily as NetCDF.
'''

import requests
import sys
import os
from dateutil import parser
from dateutil.relativedelta import relativedelta as tdelta

_valid_relativedeltatypes = ('years',
    'months',
    'weeks',
    'days',
    'hours',
    'minutes',
    'seconds')
    
class UFrame(object):
    
    def __init__(self, base_url='http://uframe-test.ooi.rutgers.edu', port=12576, timeout=10):
        self._base_url = base_url
        self._port = port
        self._timeout = timeout
        self._url = '{:s}:{:d}/sensor/inv'.format(self.base_url, self.port)
       
    @property
    def base_url(self):
        return self._base_url
    @base_url.setter
    def base_url(self, url):
        self._base_url = url
        self._url = '{:s}:{:d}/sensor/inv'.format(self._base_url, self._port)
   
    @property
    def port(self):
        return self._port
    @port.setter
    def port(self, port):
        self._port = port

    @property
    def timeout(self):
        return self._timeout
    @timeout.setter
    def timeout(self, value):
        self._port = value
        self._url = '{:s}:{:d}/sensor/inv'.format(self._base_url, self._port)

    @property
    def url(self):
        return self._url

    def __repr__(self):
        return '<UFrame(url={:s})>'.format(self.url)
        
def get_arrays(array_id=None, uframe_base=UFrame()):
    
    arrays = []
    
    try:
        r = requests.get(uframe_base.url, timeout=uframe_base.timeout)
    except (requests.Timeout, requests.ConnectionError) as e:
        sys.stderr.write('{:s}: {:s}\n'.format(e.message[0], uframe_base.url))
        return arrays
    
    if r.status_code != 200:
        sys.stderr.write('Request failed: {:s} ({:s})\n'.format(r.reason, uframe_base.url))
        return arrays
    
    arrays = r.json()
    if not array_id:
        return arrays
    
    if array_id in arrays:
        return [array_id]
    
    return []
    
def get_platforms(array_id, uframe_base=UFrame()):
    
    platforms = []
    
    if not array_id:
        sys.stderr.write('No array ID specified\n')
        return platforms
        
    url = uframe_base.url + '/{:s}'.format(array_id)
    
    try:
        r = requests.get(url, timeout=uframe_base.timeout)
    except (requests.Timeout, requests.ConnectionError) as e:
        sys.stderr.write('{:s}: {:s}\n'.format(e.message[0], url))
        return platforms
    
    if r.status_code != 200:
        sys.stderr.write('Request failed: {:s} ({:s})\n'.format(r.reason, url))
        return platforms
        
    return r.json()
    
def get_platform_streams(array_id, platform, uframe_base=UFrame()):
    
    streams = []
    
    if not array_id:
        sys.stderr.write('No array ID specified\n')
        return streams
    elif not platform:
        sys.stderr.write('No platform specified\n')
        return streams 
        
    url = uframe_base.url + '/{:s}/{:s}'.format(array_id, platform)
    
    try:
        r = requests.get(url, timeout=uframe_base.timeout)
    except (requests.Timeout, requests.ConnectionError) as e:
        sys.stderr.write('{:s}: {:s}\n'.format(e.message[0], url))
        return streams
    
    if r.status_code != 200:
        sys.stderr.write('Request failed: {:s} ({:s})\n'.format(r.reason, url))
        return streams
        
    return r.json()
    
def get_stream_metadata(array_id, platform, stream, uframe_base=UFrame()):
    
    metadata = {}
    
    if not array_id:
        sys.stderr.write('No array ID specified.\n')
        return metadata
    elif not platform:
        sys.stderr.write('No platform specified\n')
        return metadata
    elif not stream:
        sys.stderr.write('No stream specified\n')
        return metadata
    
    url = uframe_base.url + '/{:s}/{:s}/{:s}/metadata'.format(array_id,
        platform,
        stream)
    
    try:
        r = requests.get(url, timeout=uframe_base.timeout)
    except (requests.Timeout, requests.ConnectionError) as e:
        sys.stderr.write('{:s}: {:s}\n'.format(e.message[0], url))
        return metadata
    
    if r.status_code != 200:
        sys.stderr.write('Request failed: {:s} ({:s})\n'.format(r.reason, url))
        return metadata
        
    return r.json()

def get_uframe_array_nc(array_id, nc_dest=None, exec_dpa=True, urlonly=False, deltatype='days', deltaval=1, uframe_base=UFrame()):
    '''
    Download NetCDF files for the most recent 1-week worth of data for telemetered 
    and recovered data streams for the specified array_id.
    
    Args:
        array_id: name of the array
        nc_dest: top-level directory destination for writing NetCDF files.
            Defaults to the current working directory.
        exec_dpa: set to False to NOT execute L1/L2 data product algorithms prior
            to download.  Defaults to True
            
    Returns:
        urls: array of dictionaries containing the url, response code and reason
    '''
    
    fetched_urls = []
    
    if deltatype not in _valid_relativedeltatypes:
        sys.stderr.write('Invalid dateutil.relativedelta type: {:s}\n'.format(deltatype))
        sys.stderr.flush()
        return fetched_urls
        
    if not array_id:
        sys.stderr.write('Invalid array id specified\n')
        sys.stderr.flush()
        return
    if not urlonly and not nc_dest:
        nc_dest = os.path.realpath(os.curdir)
        
    if not urlonly and not os.path.exists(nc_dest):
        sys.stdout.write('Creating NetCDF destination: {:s}\n'.format(nc_dest))
        sys.stdout.flush()
        try:
            os.makedirs(nc_dest)
        except OSError as e:
            sys.stderr.write(e)
            sys.stderr.flush()
            return
        
    # Make sure the array is in uFrame
    sys.stdout.write('Fetching arrays ({:s})\n'.format(uframe_base))
    sys.stdout.flush()
    arrays = get_arrays(array_id=array_id, uframe_base=uframe_base)
    if not arrays:
        sys.stderr.write('Array {:s} does not exist in uFrame\n'.format(array_id))
        sys.stderr.flush()
        return
    
    array = arrays[0]    
    sys.stdout.write('{:s}: Array exists...\n'.format(array))
    sys.stdout.flush()
    
    # Fetch the platforms on the array
    sys.stdout.write('Fetching array platforms ({:s})\n'.format(uframe_base))
    sys.stdout.flush()
    platforms = get_platforms(array, uframe_base=uframe_base)
    if not platforms:
        sys.stderr.write('{:s}: No platforms found for specified array\n'.format(array))
        sys.stderr.flush()
        return
        
    for platform in platforms:
        
        p_name = '{:s}-{:s}'.format(array, platform, uframe_base=uframe_base)
        sys.stdout.write('{:s}: Fetching platform data streams ({:s})\n'.format(p_name, uframe_base))
        sys.stdout.flush()
        
        streams = get_platform_streams(array, platform, uframe_base=uframe_base)
        if not streams:
            sys.stderr.write('{:s}: No data streams found for this platform\n'.format(p_name))
            sys.stderr.flush()
            continue
        
        sys.stdout.write('{:s}: {:d} streams fetched\n'.format(p_name, len(streams)))
        sys.stdout.flush()
            
        sys.stdout.write('Fetching platform streams ({:s})\n'.format(uframe_base))
        sys.stdout.flush()
        for stream in streams:
            # Fetch stream metadata
            
            meta = get_stream_metadata(array, platform, stream, uframe_base=uframe_base)
            if not meta:
                sys.stderr.write('{:s}: No metadata found for stream: {:s}\n'.format(p_name, stream))
                sys.stderr.flush()
                continue
                
            for metadata in meta['times']:
                dt1 = parser.parse(metadata['endTime']) 
                dt0 = dt1 - tdelta(**dict({deltatype : deltaval}))
                ts1 = metadata['endTime']
                ts0 = dt0.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                sensor = metadata['stream']
                mode = metadata['method']

                url_template = '{:s}/{:s}/{:s}/{:s}/{:s}/{:s}?beginDT={:s}&endDT={:s}'
                # Execute data product algorithms if specified (default)
                if exec_dpa:
                    url_template += '&execDPA=true'
                # Responses as NetCDF
                url_template += '&format=application/netcdf'
                    
                nc_url = url_template.format(
                    uframe_base.url,
                    array,
                    platform,
                    stream,
                    mode,
                    sensor,
                    ts0,
                    ts1)
                
                # If urlonly is True, do not attempt to fetch.  Just append the
                # url for this stream
                fetched_urls.append({'url' : nc_url,
                    'reason' : None,
                    'code' : -1})
             
                if urlonly:
                    continue
                
                # Create the destination directory for this NetCDF file
                dest_dir = os.path.join(nc_dest, p_name, mode)
                if not os.path.exists(dest_dir):
                    sys.stdout.write('Creating NetCDF destination: {:s}\n'.format(dest_dir))
                    sys.stdout.flush()
                    try:
                        os.makedirs(dest_dir)
                    except OSError as e:
                        sys.stderr.write(e)
                        continue
                
                # Create the file name
                nc_fname = '{:s}-{:s}-{:s}-{:s}-{:s}.nc'.format(
                    p_name,
                    sensor,
                    mode,
                    dt0.strftime('%Y%m%dT%H%M%S'),
                    dt1.strftime('%Y%m%dT%H%M%S'))
                       
                # Attempt to download the file
                sys.stdout.write('Fetching url: {:s}\n'.format(nc_url))
                sys.stdout.flush()
                try:
                    r = requests.get(nc_url, stream=True, timeout=uframe_base.timeout)
                except (requests.Timeout, requests.ConnectionError) as e:
                    sys.stderr.write('{:s}: {:s}\n'.format(e.message[0], nc_url))
                    sys.stderr.flush()
                    fetched_urls[-1]['reason'] = 'ConnectTimeout'
                    fetched_urls[-1]['code'] = 500
                    continue

                fetched_urls[-1]['reason'] = r.reason
                fetched_urls[-1]['code'] = r.status_code
                if r.status_code != 200:
                    sys.stderr.write('Download failed: {:s}\n'.format(nc_url))
                    sys.stderr.flush()
                    continue
                
                # Write the file if the request succeeded
                nc_file = os.path.join(dest_dir, nc_fname)
                sys.stdout.write('Writing NetCDF: {:s}\n'.format(nc_file))
                sys.stdout.flush()
                fid = open(nc_file, 'wb')
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        fid.write(chunk)
                        fid.flush()
                    
                fid.close()              
                
    return fetched_urls
