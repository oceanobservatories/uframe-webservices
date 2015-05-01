#uframe-webservices

Set of python scripts for:
- Creating and executing uFrame data requests
- Downloading uFrame ingested data sets as NetCDF files

##Contents
- [Introduction](#introduction)
- [Installation](#installation)
- [Scripts](#scripts)
- [Examples](#examples)

###Introduction
uFrame provides web services that allow clients to create and execute requests for OOI array metadata as well as retrieving subsets of ingested data as both JSON and NetCDF files.  These scripts, based upon ingested data, create requests that can then be executed to retrieve the subsetting datasets.

###Installation
    > git clone https://github.com/ooi-integration/uframe-webservices.git

There's also a [pip_requirements.txt](https://github.com/ooi-integration/uframe-webservices/blob/master/pip_requirements.txt) containing the required packages.  To install these packages, use:

    > pip install -r pip_requirements.txt
    
###Scripts
There are 2 main scripts:
- [get_arrays.py](https://github.com/ooi-integration/uframe-webservices/blob/master/get_arrays.py): Retrieves the list of platforms for which uFrame has ingested some portion of the datasets.
- [download_uframe_platform_nc.py](https://github.com/ooi-integration/uframe-webservices/blob/master/download_uframe_platform_nc.py): Create (and optionally send) the requests for all data streams under the specified platform.

The default uFrame instance is <b>http://uframe-test.ooi.rutgers.edu</b>.  This can be changed using the <b>base_url</b> option from either of the scripts above.

###Examples

To get the list of platforms for the default uFrame instance:

    > get_arrays.py
    Available arrays:
    CP02PMUI
    CP01CNSM
    CP05MOAS

Then, to display the list of all valid data queries for a specified platform:

    > download_uframe_platform_nc.py --urlonly CP02PMUI

Or, to download the last 1 day's worth of data, as NetCDF files, to /tmp/data:

    > download_uframe_platform_nc.py --dest /tmp/data CP02PMUI

More doco avaialable via:

    > get_arrays.py -h
and:

    > download_uframe_platform_nc.py -h

