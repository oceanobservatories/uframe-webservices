"""
Module for querying uFrame instances and downloading responses, primarily as NetCDF.
"""

import requests
import sys
import os
import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta as tdelta


HTTP_STATUS_OK = 200

_valid_relativedeltatypes = ('years',
    'months',
    'weeks',
    'days',
    'hours',
    'minutes',
    'seconds')

__filename_extension = { 'netcdf':'nc', 'json':'json', 'zip':'zip' }


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
        self._url = '{:s}:{:d}/sensor/inv'.format(self.base_url, self.port)

    @property
    def port(self):
        return self._port
    @port.setter
    def port(self, port):
        self._port = port
        self._url = '{:s}:{:d}/sensor/inv'.format(self.base_url, self.port)

    @property
    def timeout(self):
        return self._timeout
    @timeout.setter
    def timeout(self, value):
        self._timeout = value

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

    if r.status_code != HTTP_STATUS_OK:
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

    if r.status_code != HTTP_STATUS_OK:
        sys.stderr.write('Request failed: {:s} ({:s})\n'.format(r.reason, url))
        return platforms

    return r.json()

def get_platform_sensors(array_id, platform, uframe_base=UFrame()):

    sensors = []

    if not array_id:
        sys.stderr.write('No array ID specified\n')
        return sensors
    elif not platform:
        sys.stderr.write('No platform specified\n')
        return sensors

    url = uframe_base.url + '/{:s}/{:s}'.format(array_id, platform)

    try:
        r = requests.get(url, timeout=uframe_base.timeout)
    except (requests.Timeout, requests.ConnectionError) as e:
        sys.stderr.write('{:s}: {:s}\n'.format(e.message[0], url))
        return sensors

    if r.status_code != HTTP_STATUS_OK:
        sys.stderr.write('Request failed: {:s} ({:s})\n'.format(r.reason, url))
        return sensors

    return r.json()

def get_sensor_metadata(array_id, platform, sensor, uframe_base=UFrame()):

    metadata = {}

    if not array_id:
        sys.stderr.write('No array ID specified.\n')
        return metadata
    elif not platform:
        sys.stderr.write('No platform specified\n')
        return metadata
    elif not sensor:
        sys.stderr.write('No sensor specified\n')
        return metadata

    url = uframe_base.url + '/{:s}/{:s}/{:s}/metadata'.format(
        array_id,
        platform,
        sensor
    )

    try:
        r = requests.get(url, timeout=uframe_base.timeout)
    except (requests.Timeout, requests.ConnectionError) as e:
        sys.stderr.write('{:s}: {:s}\n'.format(e.message[0], url))
        return metadata

    if r.status_code != HTTP_STATUS_OK:
        sys.stderr.write('Request failed: {:s} ({:s})\n'.format(r.reason, url))
        return metadata

    return r.json()


def get_uframe_array(array_id, out_dir=None, exec_dpa=True, urlonly=False, deltatype='days', deltaval=1, provenance=False, limit=True, uframe_base=UFrame(), file_format='netcdf'):
    """
    Download NetCDF / JSON files for the most recent 1-day worth of data for telemetered
    and recovered data streams for the specified array_id.

    Args:
        array_id: name of the array
        out_dir: top-level directory destination for writing NetCDF / JSON files.
            Defaults to the current working directory.
        exec_dpa: set to False to NOT execute L1/L2 data product algorithms prior
            to download.  Defaults to True

    Returns:
        urls: array of dictionaries containing the url, response code and reason
    """

    fetched_urls = []

    if deltatype not in _valid_relativedeltatypes:
        sys.stderr.write('Invalid dateutil.relativedelta type: {:s}\n'.format(deltatype))
        sys.stderr.flush()
        return fetched_urls

    if not array_id:
        sys.stderr.write('Invalid array id specified\n')
        sys.stderr.flush()
        return
    if not urlonly and not out_dir:
        out_dir = os.path.realpath(os.curdir)

    if not urlonly and not os.path.exists(out_dir):
        sys.stdout.write('Creating output directory: {:s}\n'.format(out_dir))
        sys.stdout.flush()
        try:
            os.makedirs(out_dir)
        except OSError as e:
            sys.stderr.write(str(e))
            sys.stderr.flush()
            return

    # Make sure the array is in uFrame
    if not urlonly:
        sys.stdout.write('Fetching arrays ({:s})\n'.format(uframe_base))
        sys.stdout.flush()

    arrays = get_arrays(array_id=array_id, uframe_base=uframe_base)
    if not arrays:
        sys.stderr.write('Array {:s} does not exist in uFrame\n'.format(array_id))
        sys.stderr.flush()
        return

    array = arrays[0]
    if not urlonly:
        sys.stdout.write('{:s}: Array exists...\n'.format(array))
        sys.stdout.flush()

    # Fetch the platforms on the array
    if not urlonly:
        sys.stdout.write('Fetching array platforms ({:s})\n'.format(uframe_base))
        sys.stdout.flush()

    platforms = get_platforms(array, uframe_base=uframe_base)
    if not platforms:
        sys.stderr.write('{:s}: No platforms found for specified array\n'.format(array))
        sys.stderr.flush()
        return
    
    if limit == True:
        limit = 10000 # limit to 10000 points
    else:
        limit = -1 # no limit

    for platform in platforms:

        p_name = '{:s}-{:s}'.format(array, platform)
        if not urlonly:
            sys.stdout.write('{:s}: Fetching platform data sensors ({:s})\n'.format(p_name, uframe_base))
            sys.stdout.flush()

        sensors = get_platform_sensors(array, platform, uframe_base=uframe_base)
        if not sensors:
            sys.stderr.write('{:s}: No data sensors found for this platform\n'.format(p_name))
            sys.stderr.flush()
            continue

        if not urlonly:
            sys.stdout.write('{:s}: {:d} sensors fetched\n'.format(p_name, len(sensors)))
            sys.stdout.flush()

        if not urlonly:
            sys.stdout.write('Fetching platform sensors ({:s})\n'.format(uframe_base))
            sys.stdout.flush()
        for sensor in sensors:
            # Fetch sensor metadata

            meta = get_sensor_metadata(array, platform, sensor, uframe_base=uframe_base)
            if not meta:
                sys.stderr.write('{:s}: No metadata found for sensor: {:s}\n'.format(p_name, sensor))
                sys.stderr.flush()
                continue

            for metadata in meta['times']:
                dt1 = parser.parse(metadata['endTime'])
                if dt1.year < 2000:
                    sys.stderr.write('{:s}: Invalid metadata endTime: {:s}\n'.format(p_name, metadata['endTime']))
                    sys.stderr.flush()
                    continue

                dt0 = dt1 - tdelta(**dict({deltatype : deltaval}))
                ts1 = metadata['endTime']
                ts0 = dt0.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                stream = metadata['stream']
                method = metadata['method']
                dest_dir = os.path.join(out_dir, p_name, method) if not urlonly else None

                fetched_url = fetch_uframe_time_bound_stream(
                    uframe_base = uframe_base,
                    subsite = array,
                    node = platform,
                    sensor = sensor,
                    method = method,
                    stream = stream,
                    begin_datetime = ts0,
                    end_datetime = ts1,
                    file_format = file_format,
                    exec_dpa = exec_dpa,
                    urlonly = urlonly,
                    dest_dir = dest_dir,
                    provenance = provenance,
                    limit = str(limit)
                )
                fetched_urls.append(fetched_url)

    return fetched_urls


def fetch_uframe_time_bound_stream(uframe_base, subsite, node, sensor, method, stream, begin_datetime, end_datetime,
                                     file_format, exec_dpa, urlonly, dest_dir, provenance, limit):
       
    url = '{:s}/{:s}/{:s}/{:s}/{:s}/{:s}?beginDT={:s}&endDT={:s}&format=application/{:s}&execDPA={:s}&limit={:s}&include_provenance={:s}'.format(
        uframe_base.url,
        subsite,
        node,
        sensor,
        method,
        stream,
        begin_datetime,
        end_datetime,
        file_format,
        str(exec_dpa).lower(),
        limit,
        str(provenance).lower()
    )

    fetched_url = {
        'url' : url,
        'reason' : None,
        'code' : -1,
        'request_time' : datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    }

    #file_name = '{:s}-{:s}-{:s}-{:s}-{:s}-{:s}.{:s}'.format(
    #    subsite,
    #    node,
    #    stream,
    #    method,
    #    parser.parse(begin_datetime).strftime('%Y%m%dT%H%M%S'),
    #    parser.parse(end_datetime).strftime('%Y%m%dT%H%M%S'),
    #    __filename_extension[file_format]
    #)

    # If urlonly is True, do not attempt to fetch.
    if not urlonly:

        # Create the destination directory for this file
        if not os.path.exists(dest_dir):
            sys.stdout.write('Creating destination directory: {:s}\n'.format(dest_dir))
            sys.stdout.flush()
            try:
                os.makedirs(dest_dir)
            except OSError as e:
                sys.stderr.write(str(e))
                sys.stderr.flush()

        # Attempt to download the file
        if os.path.exists(dest_dir):
            sys.stdout.write('Fetching url: {:s}\n'.format(url))
            sys.stdout.flush()
            try:
                r = requests.get(url, stream=True, timeout=uframe_base.timeout)
                fetched_url['reason'] = r.reason
                fetched_url['code'] = r.status_code
                if r.status_code == HTTP_STATUS_OK:
                    # Write the file if the request succeeded
                    
                    # 2015-07-30: kerfoot@marine.rutgers.edu
                    # Special zip-file case:
                    # if the r.headers['content-type'] == 'application/octet-stream'
                    # and r.headers['content-disposition'] ends with .zip", 
                    # override the file_format and download as zip file.  If 
                    # r.headers['content-type'] is anything else, download as the
                    # user specified format.
                    # This is a TEMPORARY patch to handle uframe returning zips
                    # of 1 or more .nc files.
                    
                    if r.headers['content-type'] == 'application/octet-stream' and r.headers['content-disposition'].endswith('.zip"'):
                        file_format = 'zip'
                        
                    file_name = '{:s}-{:s}-{:s}-{:s}-{:s}-{:s}.{:s}'.format(
                        subsite,
                        node,
                        stream,
                        method,
                        parser.parse(begin_datetime).strftime('%Y%m%dT%H%M%S'),
                        parser.parse(end_datetime).strftime('%Y%m%dT%H%M%S'),
                        __filename_extension[file_format]
                    )
                    file_path = os.path.join(dest_dir, file_name)
                    sys.stdout.write('Writing file: {:s}\n'.format(file_path))
                    sys.stdout.flush()
                    with open(file_path, 'wb') as fid:
                        for chunk in r.iter_content(chunk_size=1024):
                            if chunk:
                                fid.write(chunk)
                                fid.flush()
                else:
                    sys.stderr.write('Download failed: {:d} {:s}\n'.format(r.status_code, r.reason))
                    sys.stderr.flush()
            except (requests.Timeout, requests.ConnectionError) as e:
                sys.stderr.write('{:s}: {:s}\n'.format(e.message[0], url))
                sys.stderr.flush()
                fetched_url['reason'] = 'ConnectTimeout'
                fetched_url['code'] = 500

    return fetched_url
