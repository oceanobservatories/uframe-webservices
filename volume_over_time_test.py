#! /usr/bin/env python

from __future__ import division
import argparse
import csv
import os
import shutil
import tempfile
import time
from uframe import UFrame, fetch_uframe_time_bound_stream, HTTP_STATUS_OK


__start_time = None
__stop_time = None


def record_start_time():
    global __start_time
    __start_time = time.time()
    print 'Script started at: ', __start_time


def record_stop_time():
    global __stop_time
    __stop_time = time.time()
    print 'Script stopped at: ', __stop_time


def get_dir_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def main(args):
    """
    Uses the specified CSV file to download NetCDF / JSON files for the specified streams
    during the specified time-bounds.

    See 'example_streams.csv' for format of CSV file.

    The size in bytes of the downloaded files is calculated as well as the amount of time
    the downloads took to complete.  These values are used to calculate a Volume-Over-Time
    measurement.

    The files are downloaded to a temporary directory and deleted.

    Default uFrame instance is: http://uframe-test.ooi.rutgers.edu
    """

    file_downloads_succeeded = 0
    file_downloads_failed = 0
    temp_download_path = None

    if not args.urlonly:

        temp_download_path = tempfile.mkdtemp()

        print 'Using temp download path: {:s}'.format(temp_download_path)

        record_start_time()

    if args.base_url:
        uframe_base = UFrame(base_url=args.base_url, timeout=args.timeout)
    else:
        uframe_base = UFrame(timeout=args.timeout)

    with open(args.streams_csv) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            fetched_url = fetch_uframe_time_bound_stream(
                uframe_base = uframe_base,
                subsite = row['subsite'],
                node = row['node'],
                sensor = row['sensor'],
                method = row['method'],
                stream = row['stream'],
                begin_datetime = row['begin_datetime'],
                end_datetime = row['end_datetime'],
                file_format = args.file_format,
                exec_dpa = args.exec_dpa,
                urlonly = args.urlonly,
                dest_dir = temp_download_path
            )
            if args.urlonly:
                print fetched_url['url']
            else:
                if fetched_url['code'] == HTTP_STATUS_OK:
                    file_downloads_succeeded += 1
                else:
                    file_downloads_failed += 1

    if not args.urlonly:

        record_stop_time()

        total_bytes_downloaded = get_dir_size(temp_download_path)
        total_elapsed_time = (__stop_time - __start_time)

        print 'Deleting temp download path: ', temp_download_path
        shutil.rmtree(temp_download_path, ignore_errors=True)

        print '##############################################'
        print 'File Downloads: succeeded={0}, failed={1}'.format(file_downloads_succeeded, file_downloads_failed)
        print 'Total Bytes Downloaded: {0}'.format(total_bytes_downloaded)
        print 'Total Elapsed Time: {0} seconds'.format(total_elapsed_time)
        print 'Volume-Over-Time: {0} MB/sec'.format(total_bytes_downloaded / (1024 * 1024) / total_elapsed_time)
        print '##############################################'


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('streams_csv',
            help='Path to CSV file containing list of uFrame streams to download.')
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

    parsed_args = arg_parser.parse_args()

    main(parsed_args)
